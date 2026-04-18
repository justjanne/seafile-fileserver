// Package share manages share relations.
// share: manages personal shares and provide high level permission check functions.
package share

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/justjanne/seafile-fileserver/db"
	"github.com/justjanne/seafile-fileserver/option"
	"github.com/justjanne/seafile-fileserver/repomgr"
	log "github.com/sirupsen/logrus"
)

type group struct {
	id            int
	groupName     string
	creatorName   string
	timestamp     int64
	parentGroupID int
}

var ccnetDB *sql.DB
var seafileDB *sql.DB
var groupTableName string
var cloudMode bool

// Init ccnetDB, seafileDB, groupTableName, cloudMode
func Init(ccnet db.Database, seafile db.Database, clMode bool) {
	ccnetDB = ccnet.Connection()
	seafileDB = seafile.Connection()
	cloudMode = clMode
}

// CheckPerm get user's repo permission
func CheckPerm(repoID string, user string) string {
	var perm string
	vInfo, err := repomgr.GetVirtualRepoInfo(repoID)
	if err != nil {
		log.Errorf("Failed to get virtual repo info by repo id %s: %v", repoID, err)
	}
	if vInfo != nil {
		perm = checkVirtualRepoPerm(repoID, vInfo.OriginRepoID, user, vInfo.Path)
		return perm
	}

	perm = checkRepoSharePerm(repoID, user)

	return perm
}

func checkVirtualRepoPerm(repoID, originRepoID, user, vPath string) string {
	owner, err := repomgr.GetRepoOwner(originRepoID)
	if err != nil {
		log.Errorf("Failed to get repo owner: %v", err)
	}
	var perm string
	if owner != "" && owner == user {
		perm = "rw"
		return perm
	}
	perm = checkPermOnParentRepo(originRepoID, user, vPath)
	if perm != "" {
		return perm
	}
	perm = checkRepoSharePerm(originRepoID, user)
	return perm
}

func getUserGroups(sqlStr string, args ...interface{}) ([]group, error) {
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	rows, err := ccnetDB.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var groups []group
	var g group
	for rows.Next() {
		if err := rows.Scan(&g.id, &g.groupName,
			&g.creatorName, &g.timestamp,
			&g.parentGroupID); err == nil {

			groups = append(groups, g)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return groups, nil
}

func getGroupsByUser(userName string, returnAncestors bool) ([]group, error) {
	sqlStr := fmt.Sprintf(db.GroupFindAllByUserNameOrderByGroupIDDesc)
	groups, err := getUserGroups(sqlStr, userName)
	if err != nil {
		err := fmt.Errorf("Failed to get groups by user %s: %v", userName, err)
		return nil, err
	}
	if !returnAncestors {
		return groups, nil
	}

	var paths []string
	var ret []group
	for _, group := range groups {
		parentGroupID := group.parentGroupID
		groupID := group.id
		if parentGroupID != 0 {
			paths = append(paths, strconv.Itoa(groupID))
		} else {
			ret = append(ret, group)
		}
	}
	if len(paths) > 0 {
		paths, err := getGroupPaths(fmt.Sprintf(db.GroupStructureFindAllPathsByGroupIDIn, strings.Join(paths, ", ")))
		if err != nil {
			log.Errorf("Failed to get group paths: %v", err)
		}
		if paths == "" {
			err := fmt.Errorf("Failed to get groups path for user %s", userName)
			return nil, err
		}

		sqlStr = fmt.Sprintf(db.GroupFindAllByGroupIDInOrderByGroupIDDesc, paths)
		groups, err := getUserGroups(sqlStr)
		if err != nil {
			return nil, err
		}
		ret = append(ret, groups...)
	}
	return ret, nil
}

func getGroupPaths(sqlStr string) (string, error) {
	var paths string
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	rows, err := ccnetDB.QueryContext(ctx, sqlStr)
	if err != nil {
		return paths, err
	}

	defer rows.Close()

	var path string
	for rows.Next() {
		rows.Scan(&path)
		if paths == "" {
			paths = path
		} else {
			paths += fmt.Sprintf(", %s", path)
		}
	}

	if err := rows.Err(); err != nil {
		return "", err
	}
	return paths, nil
}

func checkGroupPermByUser(repoID string, userName string) (string, error) {
	groups, err := getGroupsByUser(userName, false)
	if err != nil {
		return "", err
	}
	if len(groups) == 0 {
		return "", nil
	}

	var groupIds []string
	for _, group := range groups {
		groupIds = append(groupIds, strconv.Itoa(group.id))
	}

	sqlStr := fmt.Sprintf(db.RepoGroupFindPermissionByRepoIDAndGroupIDIn, strings.Join(groupIds, ", "))

	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	rows, err := seafileDB.QueryContext(ctx, sqlStr, repoID)
	if err != nil {
		err := fmt.Errorf("Failed to get group permission by user %s: %v", userName, err)
		return "", err
	}

	defer rows.Close()

	var perm string
	var origPerm string
	for rows.Next() {
		if err := rows.Scan(&perm); err == nil {
			if perm == "rw" {
				origPerm = perm
			} else if perm == "r" && origPerm == "" {
				origPerm = perm
			}
		}
	}

	if err := rows.Err(); err != nil {
		err := fmt.Errorf("Failed to get group permission for user %s: %v", userName, err)
		return "", err
	}

	return origPerm, nil
}

func checkSharedRepoPerm(repoID string, email string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.QueryRowContext(ctx, db.SharedRepoFindPermissionByRepoIDAndToEmail, repoID, email)

	var perm string
	if err := row.Scan(&perm); err != nil {
		if err != sql.ErrNoRows {
			err := fmt.Errorf("Failed to check shared repo permission: %v", err)
			return "", err
		}
	}
	return perm, nil
}

func checkInnerPubRepoPerm(repoID string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.QueryRowContext(ctx, db.InnerPubRepoFindPermissionByRepoID, repoID)

	var perm string
	if err := row.Scan(&perm); err != nil {
		if err != sql.ErrNoRows {
			err := fmt.Errorf("Failed to check inner public repo permission: %v", err)
			return "", err
		}
	}

	return perm, nil
}

func checkRepoSharePerm(repoID string, userName string) string {
	owner, err := repomgr.GetRepoOwner(repoID)
	if err != nil {
		log.Errorf("Failed to get repo owner: %v", err)
	}
	if owner != "" && owner == userName {
		perm := "rw"
		return perm
	}
	perm, err := checkSharedRepoPerm(repoID, userName)
	if err != nil {
		log.Errorf("Failed to get shared repo permission: %v", err)
	}
	if perm != "" {
		return perm
	}
	perm, err = checkGroupPermByUser(repoID, userName)
	if err != nil {
		log.Errorf("Failed to get group permission by user %s: %v", userName, err)
	}
	if perm != "" {
		return perm
	}
	if !cloudMode {
		perm, err = checkInnerPubRepoPerm(repoID)
		if err != nil {
			log.Errorf("Failed to get inner pulic repo permission by repo id %s: %v", repoID, err)
			return ""
		}
		return perm
	}
	return ""
}

func getSharedDirsToUser(originRepoID string, toEmail string) (map[string]string, error) {
	dirs := make(map[string]string)

	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	rows, err := seafileDB.QueryContext(ctx, db.SharedRepoFindPathAndPermissionByToEmailAndOriginRepo, toEmail, originRepoID)
	if err != nil {
		err := fmt.Errorf("Failed to get shared directories by user %s: %v", toEmail, err)
		return nil, err
	}

	defer rows.Close()

	var path string
	var perm string
	for rows.Next() {
		if err := rows.Scan(&path, &perm); err == nil {
			dirs[path] = perm
		}
	}
	if err := rows.Err(); err != nil {
		err := fmt.Errorf("Failed to get shared directories by user %s: %v", toEmail, err)
		return nil, err
	}

	return dirs, nil
}

func getDirPerm(perms map[string]string, path string) string {
	tmp := path
	var perm string
	// If the path is empty, filepath.Dir returns ".". If the path consists entirely of separators,
	// filepath.Dir returns a single separator.
	for tmp != "/" && tmp != "." && tmp != "" {
		if perm, exists := perms[tmp]; exists {
			return perm
		}
		tmp = filepath.Dir(tmp)
	}
	return perm
}

func convertGroupListToStr(groups []group) string {
	var groupIDs strings.Builder

	for i, group := range groups {
		groupIDs.WriteString(strconv.Itoa(group.id))
		if i+1 < len(groups) {
			groupIDs.WriteString(",")
		}
	}
	return groupIDs.String()
}

func getSharedDirsToGroup(originRepoID string, groups []group) (map[string]string, error) {
	dirs := make(map[string]string)
	groupIDs := convertGroupListToStr(groups)

	sqlStr := fmt.Sprintf(db.RepoGroupFindPathAndPermissionByOriginRepoAndGroupIDIn, groupIDs)

	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	rows, err := seafileDB.QueryContext(ctx, sqlStr, originRepoID)
	if err != nil {
		err := fmt.Errorf("Failed to get shared directories: %v", err)
		return nil, err
	}

	defer rows.Close()

	var path string
	var perm string
	for rows.Next() {
		if err := rows.Scan(&path, &perm); err == nil {
			dirs[path] = perm
		}
	}

	if err := rows.Err(); err != nil {
		err := fmt.Errorf("Failed to get shared directories: %v", err)
		return nil, err
	}

	return dirs, nil
}

func checkPermOnParentRepo(originRepoID, user, vPath string) string {
	var perm string
	userPerms, err := getSharedDirsToUser(originRepoID, user)
	if err != nil {
		log.Errorf("Failed to get all shared folder perms in parent repo %.8s for user %s", originRepoID, user)
		return ""
	}
	if len(userPerms) > 0 {
		perm = getDirPerm(userPerms, vPath)
		if perm != "" {
			return perm
		}
	}

	groups, err := getGroupsByUser(user, false)
	if err != nil {
		log.Errorf("Failed to get groups by user %s: %v", user, err)
	}
	if len(groups) == 0 {
		return perm
	}

	groupPerms, err := getSharedDirsToGroup(originRepoID, groups)
	if err != nil {
		log.Errorf("Failed to get all shared folder perm from parent repo %.8s to all user groups", originRepoID)
		return ""
	}
	if len(groupPerms) == 0 {
		return ""
	}

	perm = getDirPerm(groupPerms, vPath)

	return perm
}

// SharedRepo is a shared repo object
type SharedRepo struct {
	Version      int    `json:"version"`
	ID           string `json:"id"`
	HeadCommitID string `json:"head_commit_id"`
	Name         string `json:"name"`
	MTime        int64  `json:"mtime"`
	Permission   string `json:"permission"`
	Type         string `json:"type"`
	Owner        string `json:"owner"`
	RepoType     string `json:"-"`
}

// GetReposByOwner get repos by owner
func GetReposByOwner(email string) ([]*SharedRepo, error) {
	var repos []*SharedRepo

	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	stmt, err := seafileDB.PrepareContext(ctx, db.RepoOwnerFindAllByOwnerIDOrderByUpdateTimeDescRepoID)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, email)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		repo := new(SharedRepo)
		var repoName, lastModifier, repoType sql.NullString
		if err := rows.Scan(&repo.ID, &repo.HeadCommitID,
			&repoName, &repo.Version, &repo.MTime,
			&lastModifier, &repoType); err == nil {

			if repo.HeadCommitID == "" {
				continue
			}
			if !repoName.Valid || !lastModifier.Valid {
				continue
			}
			if repoName.String == "" || lastModifier.String == "" {
				continue
			}
			repo.Name = repoName.String
			if repoType.Valid {
				repo.RepoType = repoType.String
			}
			repos = append(repos, repo)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return repos, nil
}

// ListInnerPubRepos get inner public repos
func ListInnerPubRepos() ([]*SharedRepo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	stmt, err := seafileDB.PrepareContext(ctx, db.InnerPubRepoFindAll)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var repos []*SharedRepo
	for rows.Next() {
		repo := new(SharedRepo)
		var repoName, repoType sql.NullString
		if err := rows.Scan(&repo.ID, &repo.Owner,
			&repo.Permission, &repo.HeadCommitID, &repoName,
			&repo.MTime, &repo.Version, &repoType); err == nil {

			if !repoName.Valid {
				continue
			}
			if repoName.String == "" {
				continue
			}
			repo.Name = repoName.String
			if repoType.Valid {
				repo.RepoType = repoType.String
			}
			repos = append(repos, repo)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return repos, nil
}

// ListShareRepos list share repos by email
func ListShareRepos(email, columnType string) ([]*SharedRepo, error) {
	var repos []*SharedRepo
	var query string
	if columnType == "from_email" {
		query = db.SharedRepoFindAllByFromEmailOrderByUpdateTimeDescRepoID
	} else if columnType == "to_email" {
		query = db.SharedRepoFindAllByToEmailOrderByUpdateTimeDescRepoID
	} else {
		err := fmt.Errorf("Wrong column type: %s", columnType)
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	stmt, err := seafileDB.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, email)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		repo := new(SharedRepo)
		var repoName, repoType sql.NullString
		if err := rows.Scan(&repo.ID, &repo.Owner,
			&repo.Permission, &repo.HeadCommitID,
			&repoName, &repo.MTime, &repo.Version, &repoType); err == nil {

			if !repoName.Valid {
				continue
			}
			if repoName.String == "" {
				continue
			}
			repo.Name = repoName.String
			if repoType.Valid {
				repo.RepoType = repoType.String
			}

			repos = append(repos, repo)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return repos, nil
}

// GetGroupReposByUser get group repos by user
func GetGroupReposByUser(user string, orgID int) ([]*SharedRepo, error) {
	groups, err := getGroupsByUser(user, true)
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return nil, nil
	}

	var query string
	if orgID < 0 {
		query = db.RepoGroupFindAllByGroupIDInOrderByGroupIDDesc
	} else {
		query = db.OrgRepoGroupFindAllByGroupIDInOrderByGroupIDDesc
	}

	var paths []string
	for _, group := range groups {
		paths = append(paths, strconv.Itoa(group.id))
	}

	sqlStr := fmt.Sprintf(query, strings.Join(paths, ", "))

	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	rows, err := seafileDB.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var repos []*SharedRepo
	for rows.Next() {
		gRepo := new(SharedRepo)
		var repoType sql.NullString
		if err := rows.Scan(&gRepo.ID, &gRepo.Owner,
			&gRepo.Permission, &gRepo.HeadCommitID,
			&gRepo.Name, &gRepo.MTime, &gRepo.Version, &repoType); err == nil {
			if repoType.Valid {
				gRepo.RepoType = repoType.String
			}
			repos = append(repos, gRepo)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return repos, nil
}
