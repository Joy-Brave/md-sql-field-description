import config
import myfunc
#檔案存放路徑
path = "D:\\project\\md-sql-field-description\\result\\"
#Markdown相關class
mdFieldDesc=myfunc.MdFieldDesc(path)
#資料庫相關class
sqlConn=myfunc.SqlConn(config.db)
#
mdFieldDesc.setFilenameExtList('md').setMdTableDesc()
sqlConn.selectTable(sqlConn.fieldDescQuery)
mdFieldDesc.createMdFromSqlTable(sqlConn.getSelectResult())