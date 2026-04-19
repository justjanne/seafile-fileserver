// Package repomgr manages repo objects and file operations in repos.
package repomgr

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	// Change to non-blank imports when use
	_ "github.com/justjanne/seafile-fileserver/blockmgr"
	"github.com/justjanne/seafile-fileserver/commitmgr"
	"github.com/justjanne/seafile-fileserver/db"
	"github.com/justjanne/seafile-fileserver/option"
	log "github.com/sirupsen/logrus"
)

// Repo status
const (
	RepoStatusNormal = iota
	RepoStatusReadOnly
	NRepoStatus
)

// Repo contains information about a repo.
type Repo struct {
	ID                   string
	Name                 string
	Desc                 string
	LastModifier         string
	LastModificationTime int64
	HeadCommitID         string
	RootID               string
	IsCorrupted          bool

	// Set when repo is virtual
	VirtualInfo *VRepoInfo

	// ID for fs and block store
	StoreID string

	// Encrypted repo info
	IsEncrypted   bool
	EncVersion    int
	Magic         string
	RandomKey     string
	Salt          string
	PwdHash       string
	PwdHashAlgo   string
	PwdHashParams string
	Version       int
}

// VRepoInfo contains virtual repo information.
type VRepoInfo struct {
	RepoID       string
	OriginRepoID string
	Path         string
	BaseCommitID string
}

var seafileDB db.Database

// Init initialize status of repomgr package
func Init(ccnet db.Database, seafile db.Database) {
	seafileDB = seafile
}

// Get returns Repo object by repo ID.
func Get(id string) *Repo {
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	stmt, err := seafileDB.Connection().PrepareContext(ctx, seafileDB.Queries().RepoFindFullByRepoID)
	if err != nil {
		log.Errorf("failed to prepare sql : %s ：%v", seafileDB.Queries().RepoFindFullByRepoID, err)
		return nil
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, id)
	if err != nil {
		log.Errorf("failed to query sql : %v", err)
		return nil
	}
	defer rows.Close()

	repo := new(Repo)

	var originRepoID sql.NullString
	var path sql.NullString
	var baseCommitID sql.NullString
	if rows.Next() {
		err := rows.Scan(&repo.ID, &repo.HeadCommitID, &originRepoID, &path, &baseCommitID)
		if err != nil {
			log.Errorf("failed to scan sql rows : %v", err)
			return nil
		}
	} else {
		return nil
	}

	if repo.HeadCommitID == "" {
		log.Errorf("repo %s is corrupted", id)
		return nil
	}

	if originRepoID.Valid {
		repo.VirtualInfo = new(VRepoInfo)
		repo.VirtualInfo.RepoID = id
		repo.VirtualInfo.OriginRepoID = originRepoID.String
		repo.StoreID = originRepoID.String

		if path.Valid {
			repo.VirtualInfo.Path = path.String
		}

		if baseCommitID.Valid {
			repo.VirtualInfo.BaseCommitID = baseCommitID.String
		}
	} else {
		repo.StoreID = repo.ID
	}

	commit, err := commitmgr.Load(repo.ID, repo.HeadCommitID)
	if err != nil {
		log.Errorf("failed to load commit %s/%s : %v", repo.ID, repo.HeadCommitID, err)
		return nil
	}

	repo.Name = commit.RepoName
	repo.Desc = commit.RepoDesc
	repo.LastModifier = commit.CreatorName
	repo.LastModificationTime = commit.Ctime
	repo.RootID = commit.RootID
	repo.Version = commit.Version
	if commit.Encrypted == "true" {
		repo.IsEncrypted = true
		repo.EncVersion = commit.EncVersion
		if repo.EncVersion == 1 && commit.PwdHash == "" {
			repo.Magic = commit.Magic
		} else if repo.EncVersion == 2 {
			repo.RandomKey = commit.RandomKey
		} else if repo.EncVersion == 3 {
			repo.RandomKey = commit.RandomKey
			repo.Salt = commit.Salt
		} else if repo.EncVersion == 4 {
			repo.RandomKey = commit.RandomKey
			repo.Salt = commit.Salt
		}
		if repo.EncVersion >= 2 && commit.PwdHash == "" {
			repo.Magic = commit.Magic
		}
		if commit.PwdHash != "" {
			repo.PwdHash = commit.PwdHash
			repo.PwdHashAlgo = commit.PwdHashAlgo
			repo.PwdHashParams = commit.PwdHashParams
		}
	}

	return repo
}

// RepoToCommit converts Repo to Commit.
func RepoToCommit(repo *Repo, commit *commitmgr.Commit) {
	commit.RepoID = repo.ID
	commit.RepoName = repo.Name
	if repo.IsEncrypted {
		commit.Encrypted = "true"
		commit.EncVersion = repo.EncVersion
		if repo.EncVersion == 1 && repo.PwdHash == "" {
			commit.Magic = repo.Magic
		} else if repo.EncVersion == 2 {
			commit.RandomKey = repo.RandomKey
		} else if repo.EncVersion == 3 {
			commit.RandomKey = repo.RandomKey
			commit.Salt = repo.Salt
		} else if repo.EncVersion == 4 {
			commit.RandomKey = repo.RandomKey
			commit.Salt = repo.Salt
		}
		if repo.EncVersion >= 2 && repo.PwdHash == "" {
			commit.Magic = repo.Magic
		}
		if repo.PwdHash != "" {
			commit.PwdHash = repo.PwdHash
			commit.PwdHashAlgo = repo.PwdHashAlgo
			commit.PwdHashParams = repo.PwdHashParams
		}
	} else {
		commit.Encrypted = "false"
	}
	commit.Version = repo.Version
}

// GetEx return repo object even if it's corrupted.
func GetEx(id string) *Repo {
	repo := new(Repo)
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	stmt, err := seafileDB.Connection().PrepareContext(ctx, seafileDB.Queries().RepoFindFullExByRepoID)
	if err != nil {
		repo.IsCorrupted = true
		return repo
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, id)
	if err != nil {
		repo.IsCorrupted = true
		return repo
	}
	defer rows.Close()

	var originRepoID sql.NullString
	var path sql.NullString
	var baseCommitID sql.NullString
	if rows.Next() {
		err := rows.Scan(&repo.ID, &repo.HeadCommitID, &originRepoID, &path, &baseCommitID)
		if err != nil {
			repo.IsCorrupted = true
			return repo

		}
	} else if rows.Err() != nil {
		repo.IsCorrupted = true
		return repo
	} else {
		return nil
	}
	if originRepoID.Valid {
		repo.VirtualInfo = new(VRepoInfo)
		repo.VirtualInfo.RepoID = id
		repo.VirtualInfo.OriginRepoID = originRepoID.String
		repo.StoreID = originRepoID.String

		if path.Valid {
			repo.VirtualInfo.Path = path.String
		}

		if baseCommitID.Valid {
			repo.VirtualInfo.BaseCommitID = baseCommitID.String
		}
	} else {
		repo.StoreID = repo.ID
	}

	if repo.HeadCommitID == "" {
		repo.IsCorrupted = true
		return repo
	}

	commit, err := commitmgr.Load(repo.ID, repo.HeadCommitID)
	if err != nil {
		log.Errorf("failed to load commit %s/%s : %v", repo.ID, repo.HeadCommitID, err)
		repo.IsCorrupted = true
		return repo
	}

	repo.Name = commit.RepoName
	repo.LastModifier = commit.CreatorName
	repo.LastModificationTime = commit.Ctime
	repo.RootID = commit.RootID
	repo.Version = commit.Version
	if commit.Encrypted == "true" {
		repo.IsEncrypted = true
		repo.EncVersion = commit.EncVersion
		if repo.EncVersion == 1 {
			repo.Magic = commit.Magic
		} else if repo.EncVersion == 2 {
			repo.Magic = commit.Magic
			repo.RandomKey = commit.RandomKey
		} else if repo.EncVersion == 3 {
			repo.Magic = commit.Magic
			repo.RandomKey = commit.RandomKey
			repo.Salt = commit.Salt
		} else if repo.EncVersion == 4 {
			repo.Magic = commit.Magic
			repo.RandomKey = commit.RandomKey
			repo.Salt = commit.Salt
		}
		if commit.PwdHash != "" {
			repo.PwdHash = commit.PwdHash
			repo.PwdHashAlgo = commit.PwdHashAlgo
			repo.PwdHashParams = commit.PwdHashParams
		}
	}

	return repo
}

// GetVirtualRepoInfo return virtual repo info by repo id.
func GetVirtualRepoInfo(repoID string) (*VRepoInfo, error) {
	vRepoInfo := new(VRepoInfo)

	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.Connection().QueryRowContext(ctx, seafileDB.Queries().VirtualRepoFindByRepoID, repoID)
	if err := row.Scan(&vRepoInfo.RepoID, &vRepoInfo.OriginRepoID, &vRepoInfo.Path, &vRepoInfo.BaseCommitID); err != nil {
		if err != sql.ErrNoRows {
			return nil, err
		}
		return nil, nil
	}
	return vRepoInfo, nil
}

// GetVirtualRepoInfoByOrigin return virtual repo info by origin repo id.
func GetVirtualRepoInfoByOrigin(originRepo string) ([]*VRepoInfo, error) {
	var vRepos []*VRepoInfo
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row, err := seafileDB.Connection().QueryContext(ctx, seafileDB.Queries().VirtualRepoFindByOriginRepo, originRepo)
	if err != nil {
		return nil, err
	}
	defer row.Close()
	for row.Next() {
		vRepoInfo := new(VRepoInfo)
		if err := row.Scan(&vRepoInfo.RepoID, &vRepoInfo.OriginRepoID, &vRepoInfo.Path, &vRepoInfo.BaseCommitID); err != nil {
			if err != sql.ErrNoRows {
				return nil, err
			}
		}
		vRepos = append(vRepos, vRepoInfo)
	}

	return vRepos, nil
}

// GetEmailByToken return user's email by token.
func GetEmailByToken(repoID string, token string) (string, error) {
	var email string
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.Connection().QueryRowContext(ctx, seafileDB.Queries().RepoUserTokenFindEmailByRepoIDAndToken, repoID, token)
	if err := row.Scan(&email); err != nil {
		if err != sql.ErrNoRows {
			return email, err
		}
	}
	return email, nil
}

// GetRepoStatus return repo status by repo id.
func GetRepoStatus(repoID string) (int, error) {
	var status int = -1

	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.Connection().QueryRowContext(ctx, seafileDB.Queries().VirtualRepoFindStatusByRepoID, repoID)
	if err := row.Scan(&status); err != nil {
		if err != sql.ErrNoRows {
			return status, err
		} else {
			status = -1
		}
	}
	if status >= 0 {
		return status, nil
	}

	// Then, check repo's own status.
	row = seafileDB.Connection().QueryRowContext(ctx, seafileDB.Queries().VirtualRepoFindStatusByRepoID, repoID)
	if err := row.Scan(&status); err != nil {
		if err != sql.ErrNoRows {
			return status, err
		}
	}
	return status, nil
}

// TokenPeerInfoExists check if the token exists.
func TokenPeerInfoExists(token string) (bool, error) {
	var exists string
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.Connection().QueryRowContext(ctx, seafileDB.Queries().RepoTokenPeerInfoExistsByToken, token)
	if err := row.Scan(&exists); err != nil {
		if err != sql.ErrNoRows {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

// AddTokenPeerInfo add token peer info to RepoTokenPeerInfo table.
func AddTokenPeerInfo(token, peerID, peerIP, peerName, clientVer string, syncTime int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	if _, err := seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().RepoTokenPeerInfoSave, token, peerID, peerIP, peerName, syncTime, clientVer); err != nil {
		return err
	}
	return nil
}

// UpdateTokenPeerInfo update token peer info to RepoTokenPeerInfo table.
func UpdateTokenPeerInfo(token, peerID, clientVer string, syncTime int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	if _, err := seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().RepoTokenPeerInfoUpdatePeerIPAndSyncTimeAndClientVerByToken, peerID, syncTime, clientVer, token); err != nil {
		return err
	}
	return nil
}

// GetUploadTmpFile gets the timp file path of upload file.
func GetUploadTmpFile(repoID, filePath string) (string, error) {
	var filePathNoSlash string
	if filePath[0] == '/' {
		filePathNoSlash = filePath[1:]
	} else {
		filePathNoSlash = filePath
		filePath = "/" + filePath
	}

	var tmpFile string
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.Connection().QueryRowContext(ctx, seafileDB.Queries().WebUploadTempFilesFindTmpFilePathByRepoIDAndFilePath, repoID, filePath)
	if err := row.Scan(&tmpFile); err != nil {
		if err != sql.ErrNoRows {
			return "", err
		}
	}
	if tmpFile == "" {
		row := seafileDB.Connection().QueryRowContext(ctx, seafileDB.Queries().WebUploadTempFilesFindTmpFilePathByRepoIDAndFilePath, repoID, filePathNoSlash)
		if err := row.Scan(&tmpFile); err != nil {
			if err != sql.ErrNoRows {
				return "", err
			}
		}
	}

	return tmpFile, nil
}

// AddUploadTmpFile adds the tmp file path of upload file.
func AddUploadTmpFile(repoID, filePath, tmpFile string) error {
	if filePath[0] != '/' {
		filePath = "/" + filePath
	}

	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	_, err := seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().WebUploadTempFilesSave, repoID, filePath, tmpFile)
	if err != nil {
		return err
	}

	return nil
}

// DelUploadTmpFile deletes the tmp file path of upload file.
func DelUploadTmpFile(repoID, filePath string) error {
	var filePathNoSlash string
	if filePath[0] == '/' {
		filePathNoSlash = filePath[1:]
	} else {
		filePathNoSlash = filePath
		filePath = "/" + filePath
	}

	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	_, err := seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().WebUploadTempFilesDeleteAllByRepoIDAndFilePathIn, repoID, filePath, filePathNoSlash)
	if err != nil {
		return err
	}

	return nil
}

func setRepoCommitToDb(repoID, repoName string, updateTime int64, version int, isEncrypted string, lastModifier string) error {
	var exists int
	var encrypted int

	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.Connection().QueryRowContext(ctx, seafileDB.Queries().RepoInfoExistsByRepoID, repoID)
	if err := row.Scan(&exists); err != nil {
		if err != sql.ErrNoRows {
			return err
		}
	}
	if updateTime == 0 {
		updateTime = time.Now().Unix()
	}

	if isEncrypted == "true" {
		encrypted = 1
	}

	if exists == 1 {
		if _, err := seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().RepoInfoUpdateByRepoID, repoName, updateTime, version, encrypted, lastModifier, repoID); err != nil {
			return err
		}
	} else {
		if _, err := seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().RepoInfoSave, repoID, repoName, updateTime, version, encrypted, lastModifier); err != nil {
			return err
		}
	}

	return nil
}

// SetVirtualRepoBaseCommitPath updates the table of VirtualRepo.
func SetVirtualRepoBaseCommitPath(repoID, baseCommitID, newPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	if _, err := seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().VirtualRepoUpdateByRepoID, baseCommitID, newPath, repoID); err != nil {
		return err
	}
	return nil
}

// GetVirtualRepoIDsByOrigin return the virtual repo ids by origin repo id.
func GetVirtualRepoIDsByOrigin(repoID string) ([]string, error) {
	var id string
	var ids []string
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row, err := seafileDB.Connection().QueryContext(ctx, seafileDB.Queries().VirtualRepoFindRepoIDByOriginRepo, repoID)
	if err != nil {
		return nil, err
	}
	defer row.Close()
	for row.Next() {
		if err := row.Scan(&id); err != nil {
			if err != sql.ErrNoRows {
				return nil, err
			}
		}
		ids = append(ids, id)
	}

	return ids, nil
}

// DelVirtualRepo deletes virtual repo from database.
func DelVirtualRepo(repoID string, cloudMode bool) error {
	err := removeVirtualRepoOndisk(repoID, cloudMode)
	if err != nil {
		err := fmt.Errorf("failed to remove virtual repo on disk: %v", err)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	_, err = seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().VirtualRepoDeleteByRepoID, repoID)
	if err != nil {
		return err
	}

	return nil
}

func removeVirtualRepoOndisk(repoID string, cloudMode bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	_, err := seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().RepoDeleteByRepoID, repoID)
	if err != nil {
		return err
	}
	rows, err := seafileDB.Connection().QueryContext(ctx, seafileDB.Queries().BranchFindAllByRepoID, repoID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, id, commitID string
		if err := rows.Scan(&name, &id, &commitID); err != nil {
			if err != sql.ErrNoRows {
				return err
			}
		}
		_, err := seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().RepoHeadDeleteByBranchNameAndRepoID, name, id)
		if err != nil {
			return err
		}
		_, err = seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().BranchDeleteByNameAndRepoID, name, id)
		if err != nil {
			return err
		}
	}

	_, err = seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().RepoOwnerDeleteByRepoID, repoID)
	if err != nil {
		return err
	}

	_, err = seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().SharedRepoDeleteByRepoID, repoID)
	if err != nil {
		return err
	}

	_, err = seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().RepoGroupDeleteByRepoID, repoID)
	if err != nil {
		return err
	}
	if !cloudMode {
		_, err := seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().InnerPubRepoDeleteByRepoID, repoID)
		if err != nil {
			return err
		}
	}

	_, err = seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().RepoUserTokenDeleteByRepoID, repoID)
	if err != nil {
		return err
	}

	_, err = seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().RepoValidSinceDeleteByRepoID, repoID)
	if err != nil {
		return err
	}

	_, err = seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().RepoSizeDeleteByRepoID, repoID)
	if err != nil {
		return err
	}

	_, err = seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().GarbageReposSave, repoID)
	if err != nil {
		return err
	}

	return nil
}

// IsVirtualRepo check if the repo is a virtual reop.
func IsVirtualRepo(repoID string) (bool, error) {
	var exists int
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.Connection().QueryRowContext(ctx, seafileDB.Queries().VirtualRepoExistsByRepoID, repoID)
	if err := row.Scan(&exists); err != nil {
		if err != sql.ErrNoRows {
			return false, err
		}
		return false, nil
	}
	return true, nil

}

// GetRepoOwner get the owner of repo.
func GetRepoOwner(repoID string) (string, error) {
	var owner string

	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.Connection().QueryRowContext(ctx, seafileDB.Queries().RepoOwnerFindOwnerIDByRepoID, repoID)
	if err := row.Scan(&owner); err != nil {
		if err != sql.ErrNoRows {
			return "", err
		}
	}

	return owner, nil
}

func UpdateRepoInfo(repoID, commitID string) error {
	head, err := commitmgr.Load(repoID, commitID)
	if err != nil {
		err := fmt.Errorf("failed to get commit %s:%s", repoID, commitID)
		return err
	}

	setRepoCommitToDb(repoID, head.RepoName, head.Ctime, head.Version, head.Encrypted, head.CreatorName)

	return nil
}

func HasLastGCID(repoID, clientID string) (bool, error) {
	var exist int
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.Connection().QueryRowContext(ctx, seafileDB.Queries().LastGCIDExistsByRepoIDAndClientID, repoID, clientID)
	if err := row.Scan(&exist); err != nil {
		if err != sql.ErrNoRows {
			return false, err
		}
	}
	if exist == 0 {
		return false, nil
	}
	return true, nil
}

func GetLastGCID(repoID, clientID string) (string, error) {
	var gcID sql.NullString
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.Connection().QueryRowContext(ctx, seafileDB.Queries().LastGCIDFindGCIDByRepoIDForUpdateIdAndClientId, repoID, clientID)
	if err := row.Scan(&gcID); err != nil {
		if err != sql.ErrNoRows {
			return "", err
		}
	}

	return gcID.String, nil
}

func GetCurrentGCID(repoID string) (string, error) {
	var gcID sql.NullString
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.Connection().QueryRowContext(ctx, seafileDB.Queries().GCIDFindGCIDByRepoIDForUpdateId, repoID)
	if err := row.Scan(&gcID); err != nil {
		if err != sql.ErrNoRows {
			return "", err
		}
	}

	return gcID.String, nil
}

func RemoveLastGCID(repoID, clientID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	if _, err := seafileDB.Connection().ExecContext(ctx, seafileDB.Queries().LastGCIDDeleteByRepoIDAndClientID, repoID, clientID); err != nil {
		return err
	}
	return nil
}

func SetLastGCID(repoID, clientID, gcID string) error {
	exist, err := HasLastGCID(repoID, clientID)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	if exist {
		sqlStr := seafileDB.Queries().LastGCIDUpdateGCIDByRepoIDAndClientID
		if _, err = seafileDB.Connection().ExecContext(ctx, sqlStr, gcID, repoID, clientID); err != nil {
			return err
		}
	} else {
		sqlStr := seafileDB.Queries().LastGCIDSave
		if _, err = seafileDB.Connection().ExecContext(ctx, sqlStr, repoID, clientID, gcID); err != nil {
			return err
		}
	}
	return nil
}
