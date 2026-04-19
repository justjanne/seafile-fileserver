package quotamgr

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/justjanne/seafile-fileserver/db"
	"github.com/justjanne/seafile-fileserver/option"
	"github.com/justjanne/seafile-fileserver/repomgr"
)

// InfiniteQuota indicates that the quota is unlimited.
const (
	InfiniteQuota = -2
)

var seafileDB db.Database

// Init initialize status of repomgr package
func Init(ccnet db.Database, seafile db.Database) {
	seafileDB = seafile
}

func CheckQuota(repoID string, delta int64) (int, error) {
	if repoID == "" {
		err := fmt.Errorf("bad argumets")
		return -1, err
	}

	vInfo, err := repomgr.GetVirtualRepoInfo(repoID)
	if err != nil {
		err := fmt.Errorf("failed to get virtual repo: %v", err)
		return -1, err
	}
	rRepoID := repoID
	if vInfo != nil {
		rRepoID = vInfo.OriginRepoID
	}

	user, err := repomgr.GetRepoOwner(rRepoID)
	if err != nil {
		err := fmt.Errorf("failed to get repo owner: %v", err)
		return -1, err
	}
	if user == "" {
		err := fmt.Errorf("repo %s has no owner", repoID)
		return -1, err
	}
	quota, err := GetUserQuota(user)
	if err != nil {
		err := fmt.Errorf("failed to get user quota: %v", err)
		return -1, err
	}

	if quota == InfiniteQuota {
		return 0, nil
	}
	usage, err := GetUserUsage(user)
	if err != nil || usage < 0 {
		err := fmt.Errorf("failed to get user usage: %v", err)
		return -1, err
	}
	usage += delta
	if usage >= quota {
		return 1, nil
	}

	return 0, nil
}

func GetUserQuota(user string) (int64, error) {
	var quota int64
	sqlStr := seafileDB.Queries().UserQuotaFindQuotaByUser
	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.Connection().QueryRowContext(ctx, sqlStr, user)
	if err := row.Scan(&quota); err != nil {
		if err != sql.ErrNoRows {
			return -1, err
		}
	}

	if quota <= 0 {
		quota = option.DefaultQuota
	}

	return quota, nil
}

func GetUserUsage(user string) (int64, error) {
	var usage sql.NullInt64
	sqlStr := seafileDB.Queries().RepoOwnerFindTotalUsageByOwnerID

	ctx, cancel := context.WithTimeout(context.Background(), option.DBOpTimeout)
	defer cancel()
	row := seafileDB.Connection().QueryRowContext(ctx, sqlStr, user)
	if err := row.Scan(&usage); err != nil {
		if err != sql.ErrNoRows {
			return -1, err
		}
	}

	if usage.Valid {
		return usage.Int64, nil
	}

	return 0, nil
}
