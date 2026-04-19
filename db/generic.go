package db

import (
	"database/sql"
	"fmt"
)

func InitDatabase(config DatabaseConfig, dbName string) (Database, error) {
	switch config.Type {
	case "mysql":
		return InitMysqlDatabase(config, dbName)
	case "pgsql":
		return InitPgsqlDatabase(config, dbName)
	default:
		return nil, fmt.Errorf("failed to open database: unknown type %s", config.Type)
	}
}

type Database interface {
	Connection() *sql.DB
	Queries() *Queries
}

type Queries struct {
	BranchFindCommitIDByRepoIDAndNameMaster                     string
	BranchFindRepoIDAndCommitIDByRepoIDInAndNameMaster          string
	VirtualRepoFindRepoIDAndOriginRepoByRepoID                  string
	RepoExistsByRepoID                                          string
	GCIDFindGCIDByRepoIDForUpdate                               string
	BranchFindCommitIDByNameAndRepoIDForUpdate                  string
	BranchUpdateCommitIDByNameAndRepoID                         string
	RepoSizeFindHeadIDByRepoID                                  string
	RepoSizeSave                                                string
	RepoSizeUpdateSizeAndHeadIDByRepoID                         string
	RepoFileCountExistsByRepoID                                 string
	RepoFileCountUpdateFileCountByRepoID                        string
	RepoFileCountSave                                           string
	UserQuotaFindQuotaByUser                                    string
	RepoOwnerFindTotalUsageByOwnerID                            string
	GroupStructureFindAllPathsByGroupIDIn                       string
	GroupFindAllByGroupIDInOrderByGroupIDDesc                   string
	GroupFindAllByUserNameOrderByGroupIDDesc                    string
	RepoGroupFindPermissionByRepoIDAndGroupIDIn                 string
	SharedRepoFindPermissionByRepoIDAndToEmail                  string
	InnerPubRepoFindPermissionByRepoID                          string
	SharedRepoFindPathAndPermissionByToEmailAndOriginRepo       string
	RepoGroupFindPathAndPermissionByOriginRepoAndGroupIDIn      string
	RepoOwnerFindAllByOwnerIDOrderByUpdateTimeDescRepoID        string
	InnerPubRepoFindAll                                         string
	SharedRepoFindAllByFromEmailOrderByUpdateTimeDescRepoID     string
	SharedRepoFindAllByToEmailOrderByUpdateTimeDescRepoID       string
	RepoGroupFindAllByGroupIDInOrderByGroupIDDesc               string
	OrgRepoGroupFindAllByGroupIDInOrderByGroupIDDesc            string
	VirtualRepoFindByRepoID                                     string
	VirtualRepoFindByOriginRepo                                 string
	RepoUserTokenFindEmailByRepoIDAndToken                      string
	VirtualRepoFindStatusByRepoID                               string
	RepoInfoFindStatusByRepoID                                  string
	RepoFindFullByRepoID                                        string
	RepoFindFullExByRepoID                                      string
	RepoTokenPeerInfoExistsByToken                              string
	RepoTokenPeerInfoSave                                       string
	RepoTokenPeerInfoUpdatePeerIPAndSyncTimeAndClientVerByToken string
	WebUploadTempFilesFindTmpFilePathByRepoIDAndFilePath        string
	WebUploadTempFilesSave                                      string
	WebUploadTempFilesDeleteAllByRepoIDAndFilePathIn            string
	RepoInfoExistsByRepoID                                      string
	RepoInfoUpdateByRepoID                                      string
	RepoInfoSave                                                string
	VirtualRepoUpdateByRepoID                                   string
	VirtualRepoFindRepoIDByOriginRepo                           string
	VirtualRepoDeleteByRepoID                                   string
	RepoDeleteByRepoID                                          string
	BranchFindAllByRepoID                                       string
	RepoHeadDeleteByBranchNameAndRepoID                         string
	BranchDeleteByNameAndRepoID                                 string
	RepoOwnerDeleteByRepoID                                     string
	SharedRepoDeleteByRepoID                                    string
	RepoGroupDeleteByRepoID                                     string
	InnerPubRepoDeleteByRepoID                                  string
	RepoUserTokenDeleteByRepoID                                 string
	RepoValidSinceDeleteByRepoID                                string
	RepoSizeDeleteByRepoID                                      string
	GarbageReposSave                                            string
	mysqlGarbageReposSave                                       string
	VirtualRepoExistsByRepoID                                   string
	RepoOwnerFindOwnerIDByRepoID                                string
	LastGCIDExistsByRepoIDAndClientID                           string
	LastGCIDFindGCIDByRepoIDForUpdateIdAndClientId              string
	GCIDFindGCIDByRepoIDForUpdateId                             string
	LastGCIDDeleteByRepoIDAndClientID                           string
	LastGCIDUpdateGCIDByRepoIDAndClientID                       string
	LastGCIDSave                                                string
	SelectRepoInfoByRepoId                                      string
}
