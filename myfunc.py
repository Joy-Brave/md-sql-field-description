import os
import xlrd
from mdutils.mdutils import MdUtils
from mdutils import Html
import config
import pymssql
import re
import markdown
from bs4 import BeautifulSoup

class SqlConn():
    """
    SqlConn(db)

    Args:
        db (dict): SQL server connection information.
            db={
                'username':your username (str), 
                'password':your password (str), 
                'server':your server's ip (str), 
                'database':your database name (str)
            }

    Attributes:
        db (dict): SQL server connection information.
        fieldDescQuery (str): The SQL SELECT statement for table field description.

    """
    def __init__(self, db):
        self.db = db
        self._conn = pymssql.connect(
            server=db['server'], user=db['username'], password=db['password'], database=db['database'])
        self.fieldDescQuery  = '''
            select main.TABLE_NAME 
            , case when pks.COLUMN_NAME is not null then 'O' else '' end PK
            , main.COLUMN_NAME, main.DATA_TYPE
            +(case when CHARACTER_MAXIMUM_LENGTH is not null and CHARACTER_MAXIMUM_LENGTH!=-1 then '('+cast(CHARACTER_MAXIMUM_LENGTH as varchar)+')'
                when CHARACTER_MAXIMUM_LENGTH is not null and CHARACTER_MAXIMUM_LENGTH!=-1 then '(MAX)'
                when NUMERIC_SCALE is not null and NUMERIC_SCALE!=0 then '('+cast(NUMERIC_PRECISION as varchar)+','+cast(NUMERIC_SCALE as varchar)+')' 
                else '' end) MY_DATA_TYPE 
            , case main.IS_NULLABLE when 'NO' then 'NOT NULL' when 'YES' then '' end IS_NULLABLE	   
            from INFORMATION_SCHEMA.COLUMNS main
            left join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE pks
            on main.TABLE_SCHEMA=pks.TABLE_SCHEMA
            and main.TABLE_NAME=pks.TABLE_NAME
            and main.COLUMN_NAME=pks.COLUMN_NAME
            where main.TABLE_SCHEMA='dbo' and main.TABLE_NAME not like '%_LOG' and (upper(pks.CONSTRAINT_NAME) like 'PK%' or upper(pks.CONSTRAINT_NAME) is null)
            order by main.TABLE_NAME, main.ORDINAL_POSITION
            '''

    def createTable(self, dicts):
        """
        createTable(dicts)
        
        The function drop tables refered in ``dicts`` if they exists first and then create it with the information in ``dicts``.

        Args: 
            dicts (dict): The information of tables want to create. 
                 dict={
                     tableName (str):{
                        'pkSet': Primary keys (list), 
                        'columnSet':  Column names (list), 
                    }
                    ,...
                }

                For example:
                dict={
                     'customer_id_table':{
                        'pkSet': ['customer_id'], 
                        'columnSet':  ['customer_id', 'customer_name'], 
                    }
                    ,...
                }
            
        Returns:
            self

        """
        with self._conn.cursor() as cursor:
            try:
                for tablename in dicts:
                    print(tablename)
                    cursor.execute('''
                    drop table if exists {0}
                    '''.format(tablename))
                    self._conn.commit()
                    print('drop '+tablename)
                    cursor.execute('''
                    create table {0}
                        (
                            {1}
                            , constraint PK_{0} primary key clustered
                            (
                                {2}
                            ) 
                        )
                    '''.format(tablename, ', '.join(dicts[tablename]['columnSet']), ', '.join(dicts[tablename]['pkSet'])))
                    self._conn.commit()
                    print('create '+tablename)
                
            except:
                print('error')

                # database_connection.rollback()
                #print("Rolled back")
        return self

    def selectTable(self,query,rownumber=None):
        """
        selectTable(query[,rownumber])

        Args:
            query (str) : SQL SELECT statement. It will raise error if statement is not for SELECT.
            rownumber(int, optional) : The row numbers want to select. Default is all. 
        
        Returns:
            self
        
        Raise:
            SyntaxError: If ``query`` is not SELECT statement.
        """
        with self._conn.cursor() as cursor:
            try:
                if not query.strip().upper().startswith('SELECT') : raise SyntaxError('SQL query should start with "SELECT".')
                cursor.execute(query)
                if isinstance(rownumber,int) : 
                    self._selectResult=cursor.fetchmany(rownumber)
                else :
                    if rownumber!=None: Warning('rownumber should be int.')
                    self._selectResult=cursor.fetchall()
                
            except:
                print('error')
                #database_connection.rollback()
                #print("Rolled back")
        return self

    def getSelectResult(self):
        """
        getSelectResult()

        Returns:
            The result of setter function : "selectTable".
        
        """
        print('Result:')
        print(self._selectResult)
        return self._selectResult

    def insertIntoTable(self, tableName, valueList, columnNameList=None):      
        with self._conn.cursor() as cursor:
            s='%s'
            ss=''
            for i in range(len(valueList[0])):
                if i!=0 :ss=ss+', '
                ss=ss+s
            sqlStatement='''
            INSERT INTO {tableName} {columnNameList_str} VALUES ({var})
            '''.format(tableName=tableName,columnNameList_str='' if columnNameList==None else '('+', '.join(columnNameList)+')',
            var=ss)
            print(sqlStatement)
            failset=[]
            for value in valueList:
                try:
                    cursor.execute(sqlStatement,value)
                    self._conn.commit()
                    #print('success: '+str(value))
                except: 
                    failset.append('fail: '+str(value))
            for text in failset:
                print(text)
        return self

    def truncateTable(self, tableNameList):
        with self._conn.cursor() as cursor:
            sqlStatement='''
            TRUNCATE TABLE {tableName}
            '''
            print(sqlStatement)
            failset=[]
            for value in tableNameList:
                try:
                    cursor.execute(sqlStatement.format(tableName=value))
                    self._conn.commit()
                    #print('success: '+str(value))
                except: 
                    failset.append('fail: '+str(value))
            for text in failset:
                print(text)
        return self

    def dropTable(self, tableNameList):
        with self._conn.cursor() as cursor:
            sqlStatement='''
            DROP TABLE {tableName}
            '''
            print(sqlStatement)
            failset=[]
            for value in tableNameList:
                try:
                    cursor.execute(sqlStatement.format(tableName=value))
                    self._conn.commit()
                    #print('success: '+str(value))
                except: 
                    failset.append('fail: '+str(value))
            for text in failset:
                print(text)
        return self

    def getJsonTable(self,tableName):
        subSqlConn=SqlConn(self.db)
        columnName=subSqlConn.selectTable('''
        SELECT COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS
        where TABLE_NAME='{0}'
        and TABLE_SCHEMA='dbo'
        order by ORDINAL_POSITION'''.format(tableName)).getSelectResult()
        for i in range(len(columnName)):
            columnName[i]=columnName[i][0]
        dataset=[]    
        for row in subSqlConn.selectTable('select * from {0}'.format(tableName)).getSelectResult():
            data={}
            for i in range(len(columnName)):
                data[columnName[i]]=row[i]
            dataset.append(data)
        print(dataset)
        return dataset


class MdFieldDesc():
    """
    Args:
        path (str) : Path where files are.

    Attributes:
        path (str) : Path where files are.
    """

    def __init__(self,path):
        self.path=path
        self._directory=os.fsencode(path)
        self._getFilenameList=None
        self._getMdTableDesc=None

    def setFilenameExtList(self, extension=None):
        """
        Collect file list with appointed extension.

        Args :
            extension (str, optional): Extension of files, such as 'txt', 'md', '' and so on.
            Default is all extensions.

        Returns:
            self
        """
        filenameExtList=[]
        for file in os.listdir(self._directory):
            filename = os.fsdecode(file)
            if extension!=None:
                if not (filename.endswith('.'+extension)) or (extension=='' and re.findall(r"\.", filename)):
                    continue
            filenameExtList.append(filename)
        
        self._filenameList=filenameExtList
        return self

    def setFilenameList(self, filenameList):
        """
        Args:
            filenameList (str) : Filename List.
                For example:
                    ['test1.txt','test2.md','test3']

        Returns:
            self
        """
        self._filenameList=filenameList
        return self
    
    def getFilenameList(self):
        """
        Returns:
            The result of setter function : "setFilenameList" or "setFilenameExtList".
        
        """
        print(self._filenameList)
        return self._filenameList

    def setMdTableDesc(self, filenamels=None):
        """
        Args:
            filenamels (list, optional) : The list of filenames.
                The default is the return of getter function : "getFilenameList". 
        Returns:
            self.
            See getter function "getMdTableDesc" for more information.
        """
        filenamels=self.getFilenameList() if not filenamels else filenamels
        mdTableDesc = {}
        for filename in filenamels:
            html = markdown.markdown(open("{0}{1}".format(
                self.path, filename), "r", encoding="utf-8").read())
            content = "".join(BeautifulSoup(
                html, features="html.parser").findAll(text=True)).split('\n')
            pkSet = []
            columnSet = []
            columnDescSet = {}
            for string in content:
                string = string.strip()
                if string[0:1] != '|' or string[0:11].upper() == '|PRIMARYKEY' or not re.findall(r"\w+", string):
                    continue
                stringArr = string.split('|')
                if stringArr[1].upper() == 'O':
                    pkSet.append(stringArr[2])
                columnSet.append(stringArr[2]+" "+stringArr[4]+" "+stringArr[5])
                columnDescSet[stringArr[2]] = [stringArr[3], stringArr[6]]
            mdTableDesc[filename[10:].split('.')[0]] = {
                'pkSet': pkSet, 'columnSet': columnSet, 'columnDescSet': columnDescSet}
        
        self._mdTableDesc=mdTableDesc
        return self

    def getMdTableDesc(self):
        """
        Returns:
            The result of setter function : "setMdTableDesc".
            It is dictionary data :
                {
                     tableName (str):{
                        'pkSet': Primary keys (list), 
                        'columnSet':  Column names (list), 
                        'columnDescSet': Column description dictionary (dict)
                    }
                    ,...
                }
                , where Column description dictionary is
                    {
                        Column name : [Column chinese name (str), Column description (str)]
                    }

            For example:
                {
                    'customer_id_table':{
                        'pkSet': ['customer_id'], 
                        'columnSet':  ['customer_id', 'customer_name'],
                        'columnDescSet': {
                            'customer_id':['客戶代碼', '請輸入此客戶的唯一識別代碼。']
                            ,'customer_name':['客戶名稱','請輸入此客戶的中文名稱。']
                        }
                    }
                    ,...
                }
        """
        print(self._mdTableDesc)
        return self._mdTableDesc
    
    

    def createMdFromSqlTable(self,sqlTableFieldDescData, mdTableDesc=None):
        """
        Args:
            sqlTableFieldDescData (list) : Should use the result of sqlConn.selectTable(sqlConn.fieldDescQuery) where sqlConn is type of class : SqlConn.
        
            Markdown table description (dict) which is
                {
                    tableName : list of strings (list)
                    ,... 
                }
            
        """
        mdTableDesc=self.getMdTableDesc() if not mdTableDesc else mdTableDesc
        tableNameSet=set()
        resultDict={}
        for item in sqlTableFieldDescData:
            if item[0] not in tableNameSet:
                tableNameSet.add(item[0])
                resultDict[item[0]]=[]
            originalDesc={}
            if item[0] in mdTableDesc:
                originalDesc=mdTableDesc[item[0]]['columnDescSet']
            pk=item[1]
            colName=item[2]
            colChName=originalDesc[item[2]][0] if item[2] in originalDesc else ''
            dataType=item[3]
            notNull=item[4]
            desc=originalDesc[item[2]][1] if item[2] in originalDesc else '' 
            resultDict[item[0]].extend([pk, colName, colChName, dataType, notNull, desc])
        for tableName, contentList in resultDict.items():
            mdFile = MdUtils(self.path+'SQL-Table-'+tableName)
            rowList = ['PrimaryKey', '欄位名稱', '欄位中文名稱', '欄位類型', 'NOT NULL', '詳細說明']
            rowList.extend(contentList)
            mdFile.new_table(columns=6, rows=int(len(rowList)/6),
                                text=rowList, text_align='center')
            mdFile.create_md_file()
            
