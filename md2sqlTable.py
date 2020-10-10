import config
import myfunc
#檔案存放路徑
path="D:\\project\\md-sql-field-description\\result\\"
#要Create 的表格 markdown檔清單
filenamels=["SQL-Table-Employee_id.md"]
sqlConn=myfunc.SqlConn(config.db)
mdFieldDesc=myfunc.MdFieldDesc(path)
#DROP同名表格並重建
sqlConn.createTable(mdFieldDesc.setMdTableDesc(filenamels).getMdTableDesc())
