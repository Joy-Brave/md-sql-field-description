import config
import myfunc
#檔案存放路徑
path="D:\\project\\md-sql-field-description\\result\\"
filenamels=["SQL-Table-Employee_id.md"]
sqlConn=myfunc.SqlConn(config.db)
mdFieldDesc=myfunc.MdFieldDesc(path)
#
sqlConn.createTable(mdFieldDesc.setMdTableDesc(filenamels).getMdTableDesc())
