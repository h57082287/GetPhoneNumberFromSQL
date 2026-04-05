# Table Name: people
# Table Column
# ID PName Sex Birthday AGE Telc TelF ADDRESS Company CardID TelM TelB NickName Job OldAddress OtherAddress CompanyAddress CompanyPhone CellPhone CarInfo Bank BankNumber CreditInfo Email Source Remarks
import pandas, pyodbc, time, re, sys, os
from faker import Faker

DEBUG = False
DEVELOPMENT = False
# RANGECONTROL = False
dev_server = '.\\SQLEXPRESS'
server = '.'
database = 'yuyu3'
table = 'people'
fileName = 'A物件追蹤表-(完整版).xlsx'
output_fileName = fileName.split('.')[0] + '(輸出結果).xlsx'
# start_row = -1
# end_row = -1

# define Column
col_id = 42
col_address = 6
col_floor = 11

groupDatas = []


fake = Faker('zh_TW')

def _system_argv():
    if len(sys.argv) > 1:
        idx = 1
        argv = sys.argv
        while True:
            if argv[idx] == '-D'  or argv[idx] == '--development' :
                global DEVELOPMENT
                DEVELOPMENT = True
            elif argv[idx] == '-DD' or argv[idx] == '--debug':
                global DEBUG
                DEBUG = True
            elif argv[idx] == '-s' or argv[idx] == '--server':
                global server
                idx += 1
                server = argv[idx]
            elif argv[idx] == '-d' or argv[idx] == '--database':
                global database
                idx += 1
                database = argv[idx]
            elif argv[idx] == '-t' or argv[idx] == '--table':
                global table
                idx += 1
                table = argv[idx]
            elif argv[idx] == '-rf' or argv[idx] == '--readfile':
                global fileName
                idx += 1
                fileName = argv[idx]
            elif argv[idx] == '-wf' or argv[idx] == '--writefile':
                global output_fileName
                idx += 1
                output_fileName = argv[idx]
            elif argv[idx] == '-h' or argv[idx] == '--help':
                print('Accept parmas:')
                print('-h or --help: show all available parmas.')
                print('-D or --development: Enable develop mode for debug.')
                print('-DD or --debug: Enable debug mode for more information.')
                print('-s or --server [server ip or DNS]: Overwrite the sql server ip.')
                print('-d or --database [database name]: Overwrite the sql database name.')
                print('-t or --table [table]: Overwrite the sql table name.')
                print('-rf or --readfidle [file name]: Overwrite read file name.')
                print('-wf or --writefidle [file name]: Overwrite write file name.')
                os._exit(0)
            else:
                print('Invaild parmas, plaese use -h or --help to show available options.')
                os._exit(0)
            idx += 1
            if idx >= len(sys.argv):
                break
        print('This is your new setting:')
        print(f'server: {server if not DEVELOPMENT else dev_server}')
        print(f'database: {database}')
        print(f'table: {table}')
        print(f'Read File Name: {fileName}')
        print(f'Write File Name: {output_fileName}')
        print('------------------------------------------------------')

_system_argv()

print('*****************************************************')
print('目前設定：')
print(f'伺服器: {server if not DEVELOPMENT else dev_server}')
print(f'資料庫: {database}')
print(f'表格: {table}')
print(f'讀取文件名: {fileName}')
print(f'輸出文件名: {output_fileName}')
print('*****************************************************')
print("資料庫連線中.....")
DB = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; Server=' + (server if not DEVELOPMENT else dev_server) + '; DATABASE=' + database + ';Trusted_Connection=yes;')
print('已連接成功')
db_ptr = DB.cursor()
print('啟動資料庫完成')
print('*****************************************************')

def read_xlsx():
    file_datas = pandas.read_excel(fileName).to_numpy()
    fileSize = len(file_datas)
    for idx, data in enumerate(file_datas):
        if DEBUG:
            print(f'{idx}: {data}')
            print('It is None.' if pandas.isna(data[0]) else 'It is a vaild data.')
        if (not pandas.isna(data[0])):
            g_buf = []
            g_buf.append(data)
        else:
            g_buf.append(data)
            if (idx == fileSize-1) or (not pandas.isna(file_datas[idx+1][0])):
                groupDatas.append(g_buf)
    return len(groupDatas)

def deGroupData(idx):
    size = len(groupDatas[idx])
    datas = list(groupDatas[idx])
    return size, datas

def filterData(datas):
    output = {'id':[], 'address':[], 'floor': ''}
    for data in datas:
        if DEBUG:
            for idx,  d in enumerate(data):
                print(f'{idx}: {d}')
        #  Presonal ID
        if not pandas.isna(data[col_id]) and data[col_id] != '':
            output['id'].append(data[col_id])
        # Address
        if data[col_address] != '同上' and not pandas.isna(data[col_address]) and data[col_address] != '':
            output['address'].append(data[col_address])
        # Floor
        if not pandas.isna(data[col_floor]) and data[col_floor] != '':
            output['floor'] = data[col_floor].split('=')[0]
    return output

def randomOptions(option):
    return fake.random_element(
        elements=[
            "NULL",
            f"'{option}'",
            "''"
        ]
    )

def fakeData(num):
    for i in range(num):
        sql = f"insert into {table}(ID, PName, Sex, Birthday, AGE, Telc, TelF, ADDRESS, Company, CardID, TelM, oldAddress, CompanyAddress, Email) values ('{str(fake.unique.random_int(min=160000, max=200000))}', '{fake.name()}', '{fake.random_element(elements=['男', '女'])}', '{fake.date_of_birth(minimum_age=18, maximum_age=64)}', '{str(fake.random_int(min=18, max=64))}', {randomOptions(fake.phone_number())}, {randomOptions('')}, '{fake.address()}', {randomOptions(fake.company().replace('\'', '').replace('\"', ''))}, '{fake.ssn()}', '{fake.phone_number()}', {randomOptions(fake.address())}, {randomOptions(fake.address())}, '{fake.email()}');"
        print(f'Create Fake Data....({sql})')
        db_ptr.execute(sql)
        time.sleep(0.3)
    DB.commit()

def flatten(data):
    result = []
    for d in data:
        if (type(d) == list) :
            result.extend(flatten(d))
        else:
            result.append(d)
    return result

def queryData(ids: str, addresses: list):
    if DEBUG:
        print(f"debug ID= {ids},\ndebug addresses={addresses}")
    sql = f"select DISTINCT ADDRESS from {table}"
    for idx, id in enumerate(ids):
        if id != None and id != '' and id.strip() != '':
            if idx == 0:
                sql += f" where CardID='{id}'"
            else:
                sql += f" OR CardID='{id}'"
    sql += ';'
    if DEBUG:
        print(f"debug SQL --> {sql}")
    if sql != f"select DISTINCT ADDRESS from {table};":
        db_ptr.execute(sql)
        datas = db_ptr.fetchall()
        addr = [data[0] for data in datas]
        if DEBUG:
            print(f"Query --> {datas}")
        addresses.extend(flatten(addr))
        if DEBUG:
            print(f"Merge addresses={addresses}")
    # Checking address doesn't inclide 'None'
    addresses = [addr for addr in addresses if addr != None]
    if DEBUG:
        print(f'Filter NoneType Address --> {addresses}')
    # regex this address, if it don't has any '樓'
    print('去除重複地址....')
    addresses = list(set(addresses))
    if DEBUG:
        print(f'After Process --> {addresses}')
    print('檢查是否包含"樓"....')
    buff = []
    for addr in addresses:
        if addr != None and not hasFloor(addr):
            buff.append(addr + '1樓')
        # checking if address include 'F' needs to add new address which is include '樓'
        if 'F' in addr:
            buff.append(addr.replace('F', '樓'))
        if hasFloor(addr):
            buff.append(addr.replace('樓', 'F'))
    addresses.extend(flatten(buff))
    print('檢查完成 !!!')
    sql = f"select DISTINCT PName, Telc, TelM from {table}"
    for idx, address in enumerate(addresses):
        print(f'組合SQL查詢命令，地址 {idx} : {address}')
        if idx == 0:
            sql += f" where address='{address}'"
        else:
            sql += f" OR address='{address}'"
    sql += ';'
    if DEBUG:
        print(f"Query SQL --> {sql}")
    db_ptr.execute(sql)
    return db_ptr.fetchall()

def preparWriteFileData(datas):
    nameList = [data[0] for data in datas]
    phoneList = [data[1] for data in datas]
    cphoneList = [data[2] for data in datas]
    return nameList, phoneList, cphoneList

def isMobileNumber(data):
    pattern = r'^(09\d{8})$'
    return re.match(pattern, str(data)) != None

def preProcessData(allData, names, phones, company_phone):
    title = ['次序', '房屋品牌', '案名↓', '廣告下架，來電', '屋況', '姓名', '地址', '總價', '格局', '坪數', '屋齡', '樓別(次/數)', '種類', '退信', '車位', '登記原因日期', '設定', '註記', '下架後給別家賣日期及開發信來電', '姓名', '姓名地址電話', '地址電話', '戶籍地電話地址', '戶籍地電話', '同事簽了', '郵遞區號', '建檔日期', '信件碼', '網址', '開發信1', '開發信2', '開發信3', '開發信4', '開發信5', '開發信6', '', '', '', '全名', '第一階段謄本的ID有*號', '段建號', '地號', 'ID', '資料源', '公司名', '段號', '建號', '地號', '縣市', '鄉鎮區', '', '', '', '', '', '', '', '', '', '', '開發人員', '有無調謄本']
    if DEBUG:
        print(f'names length= {len(names)}, phone length= {len(phones)}, groupDates= {len(groupDatas)}')
    result_group = []
    result = ''
    for idx in range(len(names)):
        if idx == 0:
            if names[idx] != None and names[idx] != '':
                if (phones[idx] != None and phones[idx] != '' and phones[idx].strip() != ''):
                    if isMobileNumber(phones[idx]):
                        result_group.append(f'{names[idx]}{phones[idx]}')
                    else:
                        result_group.append(f'市話{phones[idx]}')
                if (company_phone[idx] != None and company_phone[idx] != '' and company_phone[idx].strip() != ''):
                    if isMobileNumber(company_phone[idx]):
                        result_group.append(f'{names[idx]}{company_phone[idx]}')
                    else:
                        result_group.append(f'市話{company_phone[idx]}')
        else:
            if names[idx] != None and names[idx] != '':
                if (phones[idx] != None and phones[idx] != '' and phones[idx].strip() != ''):
                    if isMobileNumber(phones[idx]):
                        result_group.append(f'{names[idx]}{phones[idx]}')
                    else:
                        result_group.append(f'市話{phones[idx]}')
                if (company_phone[idx] != None and company_phone[idx] != '' and company_phone[idx].strip() != ''):
                    if isMobileNumber(company_phone[idx]):
                        result_group.append(f'{names[idx]}{company_phone[idx]}')
                    else:
                        result_group.append(f'市話{company_phone[idx]}')
    print('去除重複姓名電話組合....')
    result_group = list(set(result_group))
    if DEBUG:
        print(f'去重後的姓名電話組合: {result_group}')
    for res in result_group:
        if DEBUG:
            print(f'處理姓名電話組合: {res}')
        result += res
    if DEBUG:
        print(f'最終姓名電話組合: {result}')
    allData[1][2] = result
    df = pandas.DataFrame(allData, columns=title)
    return df

def hasFloor(addr):
    pattern = r'([0-9]+[樓Ff)]|B[0-9]+)'
    return re.search(pattern, addr)

def run_app():
    final_output = []
    fileSize = read_xlsx()
    for idx in range(fileSize):
        print('===============開始分隔線===================')
        size, datas = deGroupData(idx)
        if DEBUG:
            print(f'size={size}\ndatas= {datas}')
        output = filterData(datas)
        if DEBUG:
            print(f'id: {output["id"]},\naddress: {output["address"]},\nfloor: {output['floor']}')
        print(f'當前身分證字號: {output["id"]}')
        pname_phonenumber = queryData(output['id'], output['address'])
        print(f'查詢結果: {pname_phonenumber}')
        if DEBUG:
            print(f"qurey data = {pname_phonenumber}")
        nameList, phoneList, cphoneList = preparWriteFileData(pname_phonenumber)
        final_output.append(preProcessData(datas, nameList, phoneList, cphoneList))
        print('===============結束分隔線===================')
    print('檔案寫入中....')
    all_df = pandas.concat(final_output, ignore_index=True)
    all_df.to_excel(output_fileName, index= False)
    print('寫檔完成 !!!')

if __name__ == '__main__':
    # fakeData(100)
    print('歡迎使用資料庫自動讀取器 ^_^ ')
    if DEVELOPMENT:
        fileName = 'A物件追蹤表-(完整版)測試版.xlsx'
        output_fileName = fileName.split('.')[0] + '(輸出結果).xlsx'
    elif DEBUG:
        fileName = 'A物件追蹤表-(完整版)1 - 複製.xlsx'
        output_fileName = fileName.split('.')[0] + '(輸出結果).xlsx'
    else:
        ans = input(f'請輸入要讀取的檔案名稱或直接Enter使用預設檔名({fileName})：')
        if (ans != '' and ans != ' '):
            fileName = ans  + '.xlsx'
            output_fileName = ans + '(輸出結果).xlsx'
    run_app()
    input('程式執行完成～～請按下Enter退出程式～～～')