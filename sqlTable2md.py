import config
import myfunc
#檔案存放路徑
path = "D:\\project\\md-sql-field-description\\result\\"
#Markdown相關class
mdFieldDesc=myfunc.MdFieldDesc(path)
#資料庫相關class
sqlConn=myfunc.SqlConn(config.db)
#將result folder中的現有Markdown檔案存取目前的欄位說明
mdFieldDesc.setFilenameExtList('md').setMdTableDesc()
#SELECT SQL 的欄位資訊
sqlConn.selectTable(sqlConn.fieldDescQuery)
#重新生成Markdown覆蓋舊檔，但同時保有舊檔案的欄位中文名稱以及詳細說明
mdFieldDesc.createMdFromSqlTable(sqlConn.getSelectResult())