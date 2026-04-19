package db

import (
	"database/sql"
	"time"

	"github.com/lib/pq"
)

type PgsqlDatabase struct {
	connection *sql.DB
	queries    Queries
}

func (db *PgsqlDatabase) Connection() *sql.DB {
	return db.connection
}

func (db *PgsqlDatabase) Queries() *Queries {
	return &db.queries
}

func InitPgsqlDatabase(config DatabaseConfig, dbName string) (Database, error) {
	var sslMode pq.SSLMode
	if config.UseTLS {
		sslMode = pq.SSLModeVerifyFull
	} else {
		sslMode = pq.SSLModeDisable
	}
	connector, err := pq.NewConnectorConfig(pq.Config{
		Host:           config.Host,
		Port:           config.Port,
		User:           config.User,
		Password:       config.Password,
		Database:       dbName,
		ConnectTimeout: 5 * time.Second,
		SSLMode:        sslMode,
	})
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)

	return &PgsqlDatabase{
		connection: db,
		queries: Queries{
			BranchFindCommitIDByRepoIDAndNameMaster:                     "SELECT commit_id FROM Branch WHERE name='master' AND repo_id=$1",
			BranchFindRepoIDAndCommitIDByRepoIDInAndNameMaster:          "SELECT repo_id, commit_id FROM Branch WHERE name='master' AND repo_id IN (%s) LOCK IN SHARE MODE",
			VirtualRepoFindRepoIDAndOriginRepoByRepoID:                  "SELECT repo_id, origin_repo FROM VirtualRepo where repo_id = $1",
			RepoExistsByRepoID:                                          "SELECT EXISTS(SELECT 1 FROM Repo WHERE repo_id=$1)",
			GCIDFindGCIDByRepoIDForUpdate:                               "SELECT gc_id FROM GCID WHERE repo_id = $1 FOR UPDATE",
			BranchFindCommitIDByNameAndRepoIDForUpdate:                  "SELECT commit_id FROM Branch WHERE name = $1 AND repo_id = $2 FOR UPDATE",
			BranchUpdateCommitIDByNameAndRepoID:                         "UPDATE Branch SET commit_id = $1 WHERE name = $2 AND repo_id = $3",
			RepoSizeFindHeadIDByRepoID:                                  "SELECT head_id FROM RepoSize WHERE repo_id=$1",
			RepoSizeSave:                                                "INSERT INTO RepoSize (repo_id, size, head_id) VALUES ($1, $2, $3)",
			RepoSizeUpdateSizeAndHeadIDByRepoID:                         "UPDATE RepoSize SET size = $1, head_id = $2 WHERE repo_id = $3",
			RepoFileCountExistsByRepoID:                                 "SELECT 1 FROM RepoFileCount WHERE repo_id=$1",
			RepoFileCountUpdateFileCountByRepoID:                        "UPDATE RepoFileCount SET file_count=$1 WHERE repo_id=$2",
			RepoFileCountSave:                                           "INSERT INTO RepoFileCount (repo_id,file_count) VALUES ($1,$2)",
			UserQuotaFindQuotaByUser:                                    "SELECT quota FROM UserQuota WHERE \"user\"=$1",
			RepoOwnerFindTotalUsageByOwnerID:                            "SELECT SUM(size) FROM RepoOwner o LEFT JOIN VirtualRepo v ON o.repo_id=v.repo_id, RepoSize WHERE owner_id=$1 AND o.repo_id=RepoSize.repo_id AND v.repo_id IS NULL",
			GroupStructureFindAllPathsByGroupIDIn:                       "SELECT path FROM GroupStructure WHERE group_id IN (%s)",
			GroupFindAllByGroupIDInOrderByGroupIDDesc:                   "SELECT g.group_id, group_name, creator_name, timestamp, parent_group_id FROM \"group\" g WHERE g.group_id IN (%s) ORDER BY g.group_id DESC",
			GroupFindAllByUserNameOrderByGroupIDDesc:                    "SELECT g.group_id, group_name, creator_name, timestamp, parent_group_id FROM \"group\" g, GroupUser u WHERE g.group_id = u.group_id AND user_name=$1 ORDER BY g.group_id DESC",
			RepoGroupFindPermissionByRepoIDAndGroupIDIn:                 "SELECT permission FROM RepoGroup WHERE repo_id = $1 AND group_id IN (%s)",
			SharedRepoFindPermissionByRepoIDAndToEmail:                  "SELECT permission FROM SharedRepo WHERE repo_id=$1 AND to_email=$2",
			InnerPubRepoFindPermissionByRepoID:                          "SELECT permission FROM InnerPubRepo WHERE repo_id=$1",
			SharedRepoFindPathAndPermissionByToEmailAndOriginRepo:       "SELECT v.path, s.permission FROM SharedRepo s, VirtualRepo v WHERE s.repo_id = v.repo_id AND s.to_email = $1 AND v.origin_repo = $2",
			RepoGroupFindPathAndPermissionByOriginRepoAndGroupIDIn:      "SELECT v.path, s.permission FROM RepoGroup s, VirtualRepo v WHERE s.repo_id = v.repo_id AND v.origin_repo = $1 AND s.group_id in (%s)",
			RepoOwnerFindAllByOwnerIDOrderByUpdateTimeDescRepoID:        "SELECT o.repo_id, b.commit_id, i.name, i.version, i.update_time, i.last_modifier, i.type FROM RepoOwner o LEFT JOIN Branch b ON o.repo_id = b.repo_id LEFT JOIN RepoInfo i ON o.repo_id = i.repo_id LEFT JOIN VirtualRepo v ON o.repo_id = v.repo_id WHERE owner_id=$1 AND v.repo_id IS NULL ORDER BY i.update_time DESC, o.repo_id",
			InnerPubRepoFindAll:                                         "SELECT InnerPubRepo.repo_id, owner_id, permission, commit_id, i.name, i.update_time, i.version, i.type FROM InnerPubRepo LEFT JOIN RepoInfo i ON InnerPubRepo.repo_id = i.repo_id, RepoOwner, Branch WHERE InnerPubRepo.repo_id=RepoOwner.repo_id AND InnerPubRepo.repo_id = Branch.repo_id AND Branch.name = 'master'",
			SharedRepoFindAllByFromEmailOrderByUpdateTimeDescRepoID:     "SELECT sh.repo_id, to_email, permission, commit_id, i.name, i.update_time, i.version, i.type FROM SharedRepo sh LEFT JOIN RepoInfo i ON sh.repo_id = i.repo_id, Branch b WHERE from_email=$1 AND sh.repo_id = b.repo_id AND b.name = 'master' ORDER BY i.update_time DESC, sh.repo_id",
			SharedRepoFindAllByToEmailOrderByUpdateTimeDescRepoID:       "SELECT sh.repo_id, from_email, permission, commit_id, i.name, i.update_time, i.version, i.type FROM SharedRepo sh LEFT JOIN RepoInfo i ON sh.repo_id = i.repo_id, Branch b WHERE to_email=$1 AND sh.repo_id = b.repo_id AND b.name = 'master' ORDER BY i.update_time DESC, sh.repo_id",
			RepoGroupFindAllByGroupIDInOrderByGroupIDDesc:               "SELECT g.repo_id, user_name, permission, commit_id, i.name, i.update_time, i.version, i.type FROM RepoGroup g LEFT JOIN RepoInfo i ON g.repo_id = i.repo_id, Branch b WHERE g.repo_id = b.repo_id AND b.name = 'master' AND group_id IN (%s) ORDER BY group_id",
			OrgRepoGroupFindAllByGroupIDInOrderByGroupIDDesc:            "SELECT g.repo_id, owner, permission, commit_id, i.name, i.update_time, i.version, i.type FROM OrgGroupRepo g LEFT JOIN RepoInfo i ON g.repo_id = i.repo_id, Branch b WHERE g.repo_id = b.repo_id AND b.name = 'master' AND group_id IN (%s) ORDER BY group_id",
			VirtualRepoFindByRepoID:                                     "SELECT repo_id, origin_repo, path, base_commit FROM VirtualRepo WHERE repo_id = $1",
			VirtualRepoFindByOriginRepo:                                 "SELECT repo_id, origin_repo, path, base_commit FROM VirtualRepo WHERE origin_repo=$1",
			RepoUserTokenFindEmailByRepoIDAndToken:                      "SELECT email FROM RepoUserToken WHERE repo_id = $1 AND token = $2",
			VirtualRepoFindStatusByRepoID:                               "SELECT i.status FROM VirtualRepo v LEFT JOIN RepoInfo i ON i.repo_id=v.origin_repo WHERE v.repo_id=$1 AND i.repo_id IS NOT NULL",
			RepoInfoFindStatusByRepoID:                                  "SELECT status FROM RepoInfo WHERE repo_id=$1",
			RepoFindFullByRepoID:                                        "SELECT r.repo_id, b.commit_id, v.origin_repo, v.path, v.base_commit FROM Repo r LEFT JOIN Branch b ON r.repo_id = b.repo_id LEFT JOIN VirtualRepo v ON r.repo_id = v.repo_id WHERE r.repo_id = $1 AND b.name = 'master'",
			RepoFindFullExByRepoID:                                      "SELECT r.repo_id, b.commit_id, v.origin_repo, v.path, v.base_commit FROM Repo r LEFT JOIN Branch b ON r.repo_id = b.repo_id LEFT JOIN VirtualRepo v ON r.repo_id = v.repo_id WHERE r.repo_id = $1 AND b.name = 'master'",
			RepoTokenPeerInfoExistsByToken:                              "SELECT token FROM RepoTokenPeerInfo WHERE token=$1",
			RepoTokenPeerInfoSave:                                       "INSERT INTO RepoTokenPeerInfo (token, peer_id, peer_ip, peer_name, sync_time, client_ver) VALUES ($1, $2, $3, $4, $5, $6)",
			RepoTokenPeerInfoUpdatePeerIPAndSyncTimeAndClientVerByToken: "UPDATE RepoTokenPeerInfo SET peer_ip=$1, sync_time=$2, client_ver=$3 WHERE token=$4",
			WebUploadTempFilesFindTmpFilePathByRepoIDAndFilePath:        "SELECT tmp_file_path FROM WebUploadTempFiles WHERE repo_id = $1 AND file_path = $2",
			WebUploadTempFilesSave:                                      "INSERT INTO WebUploadTempFiles (repo_id, file_path, tmp_file_path) VALUES ($1, $2, $3)",
			WebUploadTempFilesDeleteAllByRepoIDAndFilePathIn:            "DELETE FROM WebUploadTempFiles WHERE repo_id = $1 AND file_path IN ($2, $3)",
			RepoInfoExistsByRepoID:                                      "SELECT 1 FROM RepoInfo WHERE repo_id=$1",
			RepoInfoUpdateByRepoID:                                      "UPDATE RepoInfo SET name=$1, update_time=$2, version=$3, is_encrypted=$4, last_modifier=$5 WHERE repo_id=$6",
			RepoInfoSave:                                                "INSERT INTO RepoInfo (repo_id, name, update_time, version, is_encrypted, last_modifier) VALUES ($1, $2, $3, $4, $5, $6)",
			VirtualRepoUpdateByRepoID:                                   "UPDATE VirtualRepo SET base_commit=$1, path=$2 WHERE repo_id=$3",
			VirtualRepoFindRepoIDByOriginRepo:                           "SELECT repo_id FROM VirtualRepo WHERE origin_repo=$1",
			VirtualRepoDeleteByRepoID:                                   "DELETE FROM VirtualRepo WHERE repo_id = $1",
			RepoDeleteByRepoID:                                          "DELETE FROM Repo WHERE repo_id = $1",
			BranchFindAllByRepoID:                                       "SELECT name, repo_id, commit_id FROM Branch WHERE repo_id=$1",
			RepoHeadDeleteByBranchNameAndRepoID:                         "DELETE FROM RepoHead WHERE branch_name = $1 AND repo_id = $2",
			BranchDeleteByNameAndRepoID:                                 "DELETE FROM Branch WHERE name=$1 AND repo_id=$2",
			RepoOwnerDeleteByRepoID:                                     "DELETE FROM RepoOwner WHERE repo_id = $1",
			SharedRepoDeleteByRepoID:                                    "DELETE FROM SharedRepo WHERE repo_id = $1",
			RepoGroupDeleteByRepoID:                                     "DELETE FROM RepoGroup WHERE repo_id = $1",
			InnerPubRepoDeleteByRepoID:                                  "DELETE FROM InnerPubRepo WHERE repo_id = $1",
			RepoUserTokenDeleteByRepoID:                                 "DELETE FROM RepoUserToken WHERE repo_id = $1",
			RepoValidSinceDeleteByRepoID:                                "DELETE FROM RepoValidSince WHERE repo_id = $1",
			RepoSizeDeleteByRepoID:                                      "DELETE FROM RepoSize WHERE repo_id = $1",
			GarbageReposSave:                                            "INSERT INTO GarbageRepos (repo_id) VALUES ($1) ON CONFLICT (repo_id) DO NOTHING",
			mysqlGarbageReposSave:                                       "INSERT IGNORE INTO GarbageRepos (repo_id) VALUES ($1)",
			VirtualRepoExistsByRepoID:                                   "SELECT 1 FROM VirtualRepo WHERE repo_id = $1",
			RepoOwnerFindOwnerIDByRepoID:                                "SELECT owner_id FROM RepoOwner WHERE repo_id=$1",
			LastGCIDExistsByRepoIDAndClientID:                           "SELECT 1 FROM LastGCID WHERE repo_id = $1 AND client_id = $2",
			LastGCIDFindGCIDByRepoIDForUpdateIdAndClientId:              "SELECT gc_id FROM LastGCID WHERE repo_id = $1 AND client_id = $2",
			GCIDFindGCIDByRepoIDForUpdateId:                             "SELECT gc_id FROM GCID WHERE repo_id = $1",
			LastGCIDDeleteByRepoIDAndClientID:                           "DELETE FROM LastGCID WHERE repo_id = $1 AND client_id = $2",
			LastGCIDUpdateGCIDByRepoIDAndClientID:                       "UPDATE LastGCID SET gc_id = $1 WHERE repo_id = $2 AND client_id = $3",
			LastGCIDSave:                                                "INSERT INTO LastGCID (repo_id, client_id, gc_id) VALUES ($1, $2, $3)",
			SelectRepoInfoByRepoId:                                      "select s.head_id,s.size,f.file_count FROM RepoSize s LEFT JOIN RepoFileCount f ON s.repo_id=f.repo_id WHERE s.repo_id=$1",
		},
	}, nil
}
