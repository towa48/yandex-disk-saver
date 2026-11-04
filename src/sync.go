package src

import (
	"fmt"
	"sort"
	"strconv"
	"time"
)

// runPipeline syncs src to dst: scan both trees, compare, copy duplicates, queue transfers, then clean up the destination.
func (a *App) runPipeline() {
	defer a.stop()

	ready := a.prepareSync()
	a.mainQueue.Close()
	a.workers.Wait()

	if ready && a.ctx.Err() == nil {
		a.removeStale()
		if err := a.dst.Trash().PurgeTrash(time.Now().AddDate(0, 0, -a.cfg.TrashRetentionDays)); err != nil {
			logMessage(fmt.Sprintf("Failed to purge trash: %v", err), "error")
		}
		a.purgeOldRecords()
		logMessage("=> Sync finished.", "info")
	}
}

func (a *App) prepareSync() bool {
	logMessage(fmt.Sprintf("Scanning source: %s...", a.src.Name()), "info")
	if a.scan(a.src, true, "/") != nil {
		return false
	}
	logMessage(fmt.Sprintf("Scanning destination: %s...", a.dst.Name()), "info")
	if a.scan(a.dst, false, "/") != nil {
		return false
	}

	a.markMatching()
	a.copyDuplicates()
	if a.ctx.Err() != nil {
		return false
	}
	a.queueTransfers()
	return a.ctx.Err() == nil
}

// scan builds the tree of a storage; a folder that cannot be read is marked error and skipped.
func (a *App) scan(st Storage, source bool, dir string) error {
	if err := a.ctx.Err(); err != nil {
		return err
	}

	entries, err := st.ReadDir(dir)
	if err != nil {
		if ctxErr := a.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		logMessage(fmt.Sprintf("Directory %s (%s) was not retrieved, marked as error: %v", dir, st.Name(), err), "error")
		a.store.With(func(s *State) {
			s.tree(source).Folders[dir] = &FolderNode{Path: dir, Status: statusError, Children: []string{}}
		})
		return nil
	}
	directories, files := 0, 0
	for _, entry := range entries {
		if entry.IsDir {
			directories++
		} else {
			files++
		}
	}
	logMessage(fmt.Sprintf("directory contents for %s (%s): %d directories, %d files", dir, st.Name(), directories, files), "info")

	status := statusCompleted
	if source {
		status = statusPending
	}
	folder := &FolderNode{Path: dir, Status: statusPending, Children: []string{}}
	a.store.With(func(s *State) {
		tree := s.tree(source)
		tree.Folders[dir] = folder
		for _, entry := range entries {
			if entry.IsDir {
				folder.Children = append(folder.Children, entry.Path)
				continue
			}
			tree.Files[entry.Path] = &FileNode{
				Path: entry.Path, Size: entry.Size, MD5: entry.MD5, ModTime: entry.ModTime, Status: status,
			}
		}
	})

	for _, child := range folder.Children {
		if err := a.scan(st, source, child); err != nil {
			return err
		}
	}

	a.store.With(func(*State) { folder.Status = statusCompleted })
	return nil
}

// markMatching completes source files that already exist in the destination with the same MD5.
func (a *App) markMatching() {
	a.store.With(func(s *State) {
		for p, file := range s.Src.Files {
			if existing := s.Dst.Files[p]; existing != nil && file.MD5 != "" && file.MD5 == existing.MD5 {
				file.Status = statusCompleted
			}
		}
	})
}

// copyDuplicates fills pending files that are absent in the destination from destination files with the same MD5.
func (a *App) copyDuplicates() {
	type job struct{ path, from string }
	var jobs []job
	a.store.With(func(s *State) {
		byMD5 := map[string]string{}
		for _, p := range sortedKeys(s.Dst.Files) {
			if sum := s.Dst.Files[p].MD5; sum != "" {
				if _, exists := byMD5[sum]; !exists {
					byMD5[sum] = p
				}
			}
		}
		for _, p := range sortedKeys(s.Src.Files) {
			file := s.Src.Files[p]
			if file.Status != statusPending || file.MD5 == "" || s.Dst.Files[p] != nil || s.Dst.underFailedFolder(p) {
				continue
			}
			if from, ok := byMD5[file.MD5]; ok {
				jobs = append(jobs, job{path: p, from: from})
			}
		}
	})

	for _, j := range jobs {
		if a.ctx.Err() != nil {
			return
		}
		if err := a.dst.Copy(j.from, j.path); err != nil {
			logMessage(fmt.Sprintf("Failed to copy %s -> %s: %v", j.from, j.path, err), "error")
			continue
		}
		a.store.With(func(s *State) {
			file := s.Src.Files[j.path]
			file.Status = statusCompleted
			s.Dst.Files[j.path] = &FileNode{Path: j.path, Size: file.Size, MD5: file.MD5, ModTime: file.ModTime, Status: statusCompleted}
		})
		logMessage(fmt.Sprintf("Copied: %s -> %s", j.from, j.path), "trace")
	}
}

// queueTransfers queues changed files (if under the size limit) first, then missing ones.
func (a *App) queueTransfers() {
	type replaceRow struct {
		path    string
		srcSize int64
		dstSize int64
	}
	var replace []replaceRow
	var missing []string
	var replaceSize, totalSize int64

	a.store.With(func(s *State) {
		for _, p := range sortedKeys(s.Src.Files) {
			file := s.Src.Files[p]
			totalSize += file.Size
			if file.Status != statusPending || s.Dst.underFailedFolder(p) {
				continue
			}
			if existing := s.Dst.Files[p]; existing != nil {
				replace = append(replace, replaceRow{path: p, srcSize: file.Size, dstSize: existing.Size})
				replaceSize += file.Size
			} else {
				missing = append(missing, p)
			}
		}
	})

	if replaceSize*100 > int64(a.cfg.ReplaceWarnPercent)*totalSize {
		logMessage(fmt.Sprintf("Replacing %d files skipped: size exceeds %d%% of storage.", len(replace), a.cfg.ReplaceWarnPercent), "warn")
		var rows [][]string
		for _, row := range replace {
			rows = append(rows, []string{row.path, strconv.FormatInt(row.srcSize, 10), strconv.FormatInt(row.dstSize, 10), time.Now().Format(dateFormat)})
		}
		header := "# Replace warning\n\nFiles in the source differ from the destination (MD5 mismatch) but were not replaced: " +
			fmt.Sprintf("their total size in the source exceeds %d%% of the size of all source files.\n\n", a.cfg.ReplaceWarnPercent) +
			"| File | Size: " + a.src.Name() + ", bytes | Size: " + a.dst.Name() + ", bytes | Date |\n| --- | --- | --- | --- |\n"
		if err := a.appendRows(replaceWarnName, header, rows, true); err != nil {
			logMessage(fmt.Sprintf("Failed to write %s: %v", replaceWarnName, err), "error")
		}
		replace = nil
	}

	for _, row := range replace {
		a.mainQueue.Push(row.path)
	}
	for _, p := range missing {
		a.mainQueue.Push(p)
	}
	logMessage(fmt.Sprintf("Queued: %d replacements, %d new files.", len(replace), len(missing)), "info")
}

// transferFile copies one file from the source to the destination.
func (a *App) transferFile(file FileNode) error {
	reader, err := a.src.Open(file.Path, file.Size)
	if err != nil {
		return err
	}
	defer reader.Close()
	return a.dst.Put(file.Path, reader, file.Size, file.MD5)
}

// attempt makes one transfer attempt and updates the file status; it reports success.
func (a *App) attempt(p string) bool {
	if a.ctx.Err() != nil {
		return false
	}

	var file FileNode
	var found bool
	a.store.With(func(s *State) {
		if node := s.Src.Files[p]; node != nil {
			found, file = true, *node
		}
	})
	if !found {
		return false
	}

	logMessage(fmt.Sprintf("Transfer started: %s (%s -> %s)", p, a.src.Name(), a.dst.Name()), "trace")

	if err := a.transferFile(file); err != nil {
		logMessage(fmt.Sprintf("Error %s: %v.", p, err), "error")

		var attempts int
		a.store.With(func(s *State) {
			node := s.Src.Files[p]
			node.Status = statusError
			node.Attempts++
			node.ErrorCode = errorCode(err)
			attempts = node.Attempts
		})
		if attempts >= a.cfg.MaxFileRetries {
			a.appendDownloadError(p, &file.Size, errorCode(err))
		}
		return false
	}

	a.store.With(func(s *State) {
		s.Src.Files[p].Status = statusCompleted
		s.Dst.Files[p] = &FileNode{Path: p, Size: file.Size, MD5: file.MD5, ModTime: file.ModTime, Status: statusCompleted}
	})
	logMessage(fmt.Sprintf("Transferred: %s", p), "trace")
	return true
}

// removeStale moves destination files missing in the source to the destination trash, unless they exceed the size limit.
func (a *App) removeStale() {
	var stale []FileNode
	var staleSize, totalSize int64
	a.store.With(func(s *State) {
		for p, file := range s.Dst.Files {
			totalSize += file.Size
			// the content of a folder that failed to list is unknown, so its files are never stale
			if s.Src.Files[p] == nil && !s.Src.underFailedFolder(p) {
				stale = append(stale, *file)
				staleSize += file.Size
			}
		}
	})
	if len(stale) == 0 {
		return
	}
	sort.Slice(stale, func(i, j int) bool { return stale[i].Path < stale[j].Path })

	if staleSize*100 > int64(a.cfg.DeleteWarnPercent)*totalSize {
		logMessage(fmt.Sprintf("Files to delete (%d) exceed %d%% of storage, deletion skipped.", len(stale), a.cfg.DeleteWarnPercent), "warn")
		a.writeDeleteWarn(stale)
		return
	}

	paths := make([]string, len(stale))
	for i, file := range stale {
		paths[i] = file.Path
	}
	removed, err := a.dst.Remove(paths)
	if err != nil {
		logMessage(fmt.Sprintf("Failed to save trash records: %v", err), "error")
	}
	a.store.With(func(s *State) {
		for _, p := range removed {
			delete(s.Dst.Files, p)
		}
	})
	logMessage(fmt.Sprintf("Files moved to trash: %d", len(removed)), "info")
}

func (a *App) writeDeleteWarn(stale []FileNode) {
	var rows [][]string
	for _, file := range stale {
		rows = append(rows, []string{file.Path, strconv.FormatInt(file.Size, 10), time.Now().Format(dateFormat)})
	}
	header := "# Delete warning\n\nFiles that no longer exist in the source were not deleted (" + a.dst.Name() + "): " +
		fmt.Sprintf("their total size exceeds %d%% of the storage size.\n\n", a.cfg.DeleteWarnPercent) +
		"| File | Size, bytes | Date |\n| --- | --- | --- |\n"
	if err := a.appendRows(deleteWarnName, header, rows, true); err != nil {
		logMessage(fmt.Sprintf("Failed to write %s: %v", deleteWarnName, err), "error")
	}
}
