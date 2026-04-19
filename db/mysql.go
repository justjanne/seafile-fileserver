package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MysqlDatabase struct {
	connection *sql.DB
	queries    Queries
}

func (db *MysqlDatabase) Connection() *sql.DB {
	return db.connection
}

func (db *MysqlDatabase) Queries() *Queries {
	return &db.queries
}

func InitMysqlDatabase(config DatabaseConfig, dbName string) (Database, error) {
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=%t&readTimeout=60s&writeTimeout=60s", config.User, config.Password, config.Host, config.Port, dbName, config.UseTLS)
	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)

	return &MysqlDatabase{
		connection: db,
		queries: Queries{
			BranchFindRepoIDAndCommitIDByRepoIDInAndNameMaster: "SELECT repo_id, commit_id FROM Branch WHERE name='master' AND repo_id IN (%s) LOCK IN SHARE MODE",
			VirtualRepoFindRepoIDAndOriginRepoByRepoID:         "SELECT repo_id, origin_repo FROM VirtualRepo where repo_id = ?",
			RepoExistsByRepoID:                                          "SELECT EXISTS(SELECT 1 FROM Repo WHERE repo_id=?)",
			GCIDFindGCIDByRepoIDForUpdate:                               "SELECT gc_id FROM GCID WHERE repo_id = ? FOR UPDATE",
			BranchFindCommitIDByNameAndRepoIDForUpdate:                  "SELECT commit_id FROM Branch WHERE name = ? AND repo_id = ? FOR UPDATE",
			BranchUpdateCommitIDByNameAndRepoID:                         "UPDATE Branch SET commit_id = ? WHERE name = ? AND repo_id = ?",
			RepoSizeFindHeadIDByRepoID:                                  "SELECT head_id FROM RepoSize WHERE repo_id=?",
			RepoSizeSave:                                                "INSERT INTO RepoSize (repo_id, size, head_id) VALUES (?, ?, ?)",
			RepoSizeUpdateSizeAndHeadIDByRepoID:                         "UPDATE RepoSize SET size = ?, head_id = ? WHERE repo_id = ?",
			RepoFileCountExistsByRepoID:                                 "SELECT 1 FROM RepoFileCount WHERE repo_id=?",
			RepoFileCountUpdateFileCountByRepoID:                        "UPDATE RepoFileCount SET file_count=? WHERE repo_id=?",
			RepoFileCountSave:                                           "INSERT INTO RepoFileCount (repo_id,file_count) VALUES (?,?)",
			UserQuotaFindQuotaByUser:                                    "SELECT quota FROM UserQuota WHERE `user`=?",
			RepoOwnerFindTotalUsageByOwnerID:                            "SELECT SUM(size) FROM RepoOwner o LEFT JOIN VirtualRepo v ON o.repo_id=v.repo_id, RepoSize WHERE owner_id=? AND o.repo_id=RepoSize.repo_id AND v.repo_id IS NULL",
			GroupStructureFindAllPathsByGroupIDIn:                       "SELECT path FROM GroupStructure WHERE group_id IN (%s)",
			GroupFindAllByGroupIDInOrderByGroupIDDesc:                   "SELECT g.group_id, group_name, creator_name, timestamp, parent_group_id FROM `group` g WHERE g.group_id IN (%s) ORDER BY g.group_id DESC",
			GroupFindAllByUserNameOrderByGroupIDDesc:                    "SELECT g.group_id, group_name, creator_name, timestamp, parent_group_id FROM `group` g, GroupUser u WHERE g.group_id = u.group_id AND user_name=? ORDER BY g.group_id DESC",
			RepoGroupFindPermissionByRepoIDAndGroupIDIn:                 "SELECT permission FROM RepoGroup WHERE repo_id = ? AND group_id IN (%s)",
			SharedRepoFindPermissionByRepoIDAndToEmail:                  "SELECT permission FROM SharedRepo WHERE repo_id=? AND to_email=?",
			InnerPubRepoFindPermissionByRepoID:                          "SELECT permission FROM InnerPubRepo WHERE repo_id=?",
			SharedRepoFindPathAndPermissionByToEmailAndOriginRepo:       "SELECT v.path, s.permission FROM SharedRepo s, VirtualRepo v WHERE s.repo_id = v.repo_id AND s.to_email = ? AND v.origin_repo = ?",
			RepoGroupFindPathAndPermissionByOriginRepoAndGroupIDIn:      "SELECT v.path, s.permission FROM RepoGroup s, VirtualRepo v WHERE s.repo_id = v.repo_id AND v.origin_repo = ? AND s.group_id in (%s)",
			RepoOwnerFindAllByOwnerIDOrderByUpdateTimeDescRepoID:        "SELECT o.repo_id, b.commit_id, i.name, i.version, i.update_time, i.last_modifier, i.type FROM RepoOwner o LEFT JOIN Branch b ON o.repo_id = b.repo_id LEFT JOIN RepoInfo i ON o.repo_id = i.repo_id LEFT JOIN VirtualRepo v ON o.repo_id = v.repo_id WHERE owner_id=? AND v.repo_id IS NULL ORDER BY i.update_time DESC, o.repo_id",
			InnerPubRepoFindAll:                                         "SELECT InnerPubRepo.repo_id, owner_id, permission, commit_id, i.name, i.update_time, i.version, i.type FROM InnerPubRepo LEFT JOIN RepoInfo i ON InnerPubRepo.repo_id = i.repo_id, RepoOwner, Branch WHERE InnerPubRepo.repo_id=RepoOwner.repo_id AND InnerPubRepo.repo_id = Branch.repo_id AND Branch.name = 'master'",
			SharedRepoFindAllByFromEmailOrderByUpdateTimeDescRepoID:     "SELECT sh.repo_id, to_email, permission, commit_id, i.name, i.update_time, i.version, i.type FROM SharedRepo sh LEFT JOIN RepoInfo i ON sh.repo_id = i.repo_id, Branch b WHERE from_email=? AND sh.repo_id = b.repo_id AND b.name = 'master' ORDER BY i.update_time DESC, sh.repo_id",
			SharedRepoFindAllByToEmailOrderByUpdateTimeDescRepoID:       "SELECT sh.repo_id, from_email, permission, commit_id, i.name, i.update_time, i.version, i.type FROM SharedRepo sh LEFT JOIN RepoInfo i ON sh.repo_id = i.repo_id, Branch b WHERE to_email=? AND sh.repo_id = b.repo_id AND b.name = 'master' ORDER BY i.update_time DESC, sh.repo_id",
			RepoGroupFindAllByGroupIDInOrderByGroupIDDesc:               "SELECT g.repo_id, user_name, permission, commit_id, i.name, i.update_time, i.version, i.type FROM RepoGroup g LEFT JOIN RepoInfo i ON g.repo_id = i.repo_id, Branch b WHERE g.repo_id = b.repo_id AND b.name = 'master' AND group_id IN (%s) ORDER BY group_id",
			OrgRepoGroupFindAllByGroupIDInOrderByGroupIDDesc:            "SELECT g.repo_id, owner, permission, commit_id, i.name, i.update_time, i.version, i.type FROM OrgGroupRepo g LEFT JOIN RepoInfo i ON g.repo_id = i.repo_id, Branch b WHERE g.repo_id = b.repo_id AND b.name = 'master' AND group_id IN (%s) ORDER BY group_id",
			VirtualRepoFindByRepoID:                                     "SELECT repo_id, origin_repo, path, base_commit FROM VirtualRepo WHERE repo_id = ?",
			VirtualRepoFindByOriginRepo:                                 "SELECT repo_id, origin_repo, path, base_commit FROM VirtualRepo WHERE origin_repo=?",
			RepoUserTokenFindEmailByRepoIDAndToken:                      "SELECT email FROM RepoUserToken WHERE repo_id = ? AND token = ?",
			VirtualRepoFindStatusByRepoID:                               "SELECT i.status FROM VirtualRepo v LEFT JOIN RepoInfo i ON i.repo_id=v.origin_repo WHERE v.repo_id=? AND i.repo_id IS NOT NULL",
			RepoInfoFindStatusByRepoID:                                  "SELECT status FROM RepoInfo WHERE repo_id=?",
			RepoFindFullByRepoID:                                        "SELECT r.repo_id, b.commit_id, v.origin_repo, v.path, v.base_commit FROM Repo r LEFT JOIN Branch b ON r.repo_id = b.repo_id LEFT JOIN VirtualRepo v ON r.repo_id = v.repo_id WHERE r.repo_id = ? AND b.name = 'master'",
			RepoFindFullExByRepoID:                                      "SELECT r.repo_id, b.commit_id, v.origin_repo, v.path, v.base_commit FROM Repo r LEFT JOIN Branch b ON r.repo_id = b.repo_id LEFT JOIN VirtualRepo v ON r.repo_id = v.repo_id WHERE r.repo_id = ? AND b.name = 'master'",
			RepoTokenPeerInfoExistsByToken:                              "SELECT token FROM RepoTokenPeerInfo WHERE token=?",
			RepoTokenPeerInfoSave:                                       "INSERT INTO RepoTokenPeerInfo (token, peer_id, peer_ip, peer_name, sync_time, client_ver) VALUES (?, ?, ?, ?, ?, ?)",
			RepoTokenPeerInfoUpdatePeerIPAndSyncTimeAndClientVerByToken: "UPDATE RepoTokenPeerInfo SET peer_ip=?, sync_time=?, client_ver=? WHERE token=?",
			WebUploadTempFilesFindTmpFilePathByRepoIDAndFilePath:        "SELECT tmp_file_path FROM WebUploadTempFiles WHERE repo_id = ? AND file_path = ?",
			WebUploadTempFilesSave:                                      "INSERT INTO WebUploadTempFiles (repo_id, file_path, tmp_file_path) VALUES (?, ?, ?)",
			WebUploadTempFilesDeleteAllByRepoIDAndFilePathIn:            "DELETE FROM WebUploadTempFiles WHERE repo_id = ? AND file_path IN (?, ?)",
			RepoInfoExistsByRepoID:                                      "SELECT 1 FROM RepoInfo WHERE repo_id=?",
			RepoInfoUpdateByRepoID:                                      "UPDATE RepoInfo SET name=?, update_time=?, version=?, is_encrypted=?, last_modifier=? WHERE repo_id=?",
			RepoInfoSave:                                                "INSERT INTO RepoInfo (repo_id, name, update_time, version, is_encrypted, last_modifier) VALUES (?, ?, ?, ?, ?, ?)",
			VirtualRepoUpdateByRepoID:                                   "UPDATE VirtualRepo SET base_commit=?, path=? WHERE repo_id=?",
			VirtualRepoFindRepoIDByOriginRepo:                           "SELECT repo_id FROM VirtualRepo WHERE origin_repo=?",
			VirtualRepoDeleteByRepoID:                                   "DELETE FROM VirtualRepo WHERE repo_id = ?",
			RepoDeleteByRepoID:                                          "DELETE FROM Repo WHERE repo_id = ?",
			BranchFindAllByRepoID:                                       "SELECT name, repo_id, commit_id FROM Branch WHERE repo_id=?",
			RepoHeadDeleteByBranchNameAndRepoID:                         "DELETE FROM RepoHead WHERE branch_name = ? AND repo_id = ?",
			BranchDeleteByNameAndRepoID:                                 "DELETE FROM Branch WHERE name=? AND repo_id=?",
			RepoOwnerDeleteByRepoID:                                     "DELETE FROM RepoOwner WHERE repo_id = ?",
			SharedRepoDeleteByRepoID:                                    "DELETE FROM SharedRepo WHERE repo_id = ?",
			RepoGroupDeleteByRepoID:                                     "DELETE FROM RepoGroup WHERE repo_id = ?",
			InnerPubRepoDeleteByRepoID:                                  "DELETE FROM InnerPubRepo WHERE repo_id = ?",
			RepoUserTokenDeleteByRepoID:                                 "DELETE FROM RepoUserToken WHERE repo_id = ?",
			RepoValidSinceDeleteByRepoID:                                "DELETE FROM RepoValidSince WHERE repo_id = ?",
			RepoSizeDeleteByRepoID:                                      "DELETE FROM RepoSize WHERE repo_id = ?",
			GarbageReposSave:                                            "INSERT INTO GarbageRepos (repo_id) VALUES (?) ON CONFLICT (repo_id) DO NOTHING",
			mysqlGarbageReposSave:                                       "INSERT IGNORE INTO GarbageRepos (repo_id) VALUES (?)",
			VirtualRepoExistsByRepoID:                                   "SELECT 1 FROM VirtualRepo WHERE repo_id = ?",
			RepoOwnerFindOwnerIDByRepoID:                                "SELECT owner_id FROM RepoOwner WHERE repo_id=?",
			LastGCIDExistsByRepoIDAndClientID:                           "SELECT 1 FROM LastGCID WHERE repo_id = ? AND client_id = ?",
			LastGCIDFindGCIDByRepoIDForUpdateIdAndClientId:              "SELECT gc_id FROM LastGCID WHERE repo_id = ? AND client_id = ?",
			GCIDFindGCIDByRepoIDForUpdateId:                             "SELECT gc_id FROM GCID WHERE repo_id = ?",
			LastGCIDDeleteByRepoIDAndClientID:                           "DELETE FROM LastGCID WHERE repo_id = ? AND client_id = ?",
			LastGCIDUpdateGCIDByRepoIDAndClientID:                       "UPDATE LastGCID SET gc_id = ? WHERE repo_id = ? AND client_id = ?",
			LastGCIDSave:                                                "INSERT INTO LastGCID (repo_id, client_id, gc_id) VALUES (?, ?, ?)",
			SelectRepoInfoByRepoId:                                      "select s.head_id,s.size,f.file_count FROM RepoSize s LEFT JOIN RepoFileCount f ON s.repo_id=f.repo_id WHERE s.repo_id=?",
		},
	}, nil
}
