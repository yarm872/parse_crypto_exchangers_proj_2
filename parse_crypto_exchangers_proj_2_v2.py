import multiprocessing as mp
from selenium import webdriver
from selenium.webdriver.common.by import By
import gspread
import time

def get_data_from_google_table(): #вытягивание всей инфы из таблицы
    gc = gspread.service_account(filename='D:/MY PROGS/ПАРСЕРЫ/проект кирилла/mytest-411319-99861ed21234.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1N-eSem5yEzAFLmCveUZNtnaYVJ_lPOKOvp3yVo4LK4M/edit#gid=0')
    worksheet = sh.sheet1
    list_of_exchangers_and_urls=[]
    for i in range(1,41,2):
        values_list = worksheet.col_values(i)
        list_of_exchangers_and_urls.append(values_list)
     
    # получение коллекции обменников; по итогу оказалось не нужно    
    exchangers=worksheet.row_values(2)
    result=[]
    for i in exchangers:
        y=i.split(", ")
        result.extend(y)
    result=set(result)
    result.discard("")
    # получение коллекции обменников; по итогу оказалось не нужно  
    return list_of_exchangers_and_urls, result #2 значение по итогу оказалось не нужно

def get_direction(url): #получение направление обмена из ссылки например - BTC-RUB
    # url=https://www.bestchange.ru/bitcoin-to-cash-ruble-in-msk.html
    direction=""
    for j in url[26:]:
        if j!=".":
            direction+=j
        else:
            break
    return direction

def parse_page(url): # вытягивание всех обменников представленных по ссылке
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    #options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    driver = webdriver.Chrome(options=options)
    try:
        driver.get(url)                                          
        list_of_exchangers = driver.find_element(By.ID,"rates_block")
        list_of_exchangers = list_of_exchangers.find_element(By.ID,"content_table")
        list_of_exchangers = list_of_exchangers.find_element(By.TAG_NAME,"tbody")
        list_of_exchangers = list_of_exchangers.find_elements(By.TAG_NAME,"tr")
        #список объектов обменников на сайте получен
        names_of_exchangers_on_page = []
        for exchanger in list_of_exchangers:
            data = exchanger.find_element(By.CLASS_NAME,"bj")
            names_of_exchangers_on_page.append(data.text) #список названий обменников на сайте получен и добавлен
        
        return names_of_exchangers_on_page
        
    except Exception as ex:
        return [] 
    
def get_formated_data(element, result_structure_shared, lock):
    exchangers = element[1].split(", ")
    for url in element[2:]:
        names_of_exchangers_on_page = parse_page(url)
        for exch in exchangers:
            if exch in names_of_exchangers_on_page:
                city = element[0]
                #lock.acquire()
                try:
                    
                    for i in result_structure_shared:
                        if exch in i:
                            i[exch][city].append((url, get_direction(url), "+"))
                            break
                finally:
                    #lock.release()
                    #print(f"Released lock: {mp.current_process().name}")
                    pass
            else:
                city = element[0]
                #lock.acquire()
                try:
                    
                    for i in result_structure_shared:
                        if exch in i:
                            i[exch][city].append((url, get_direction(url), "-"))
                            break
                finally:
                    #lock.release()
                    #print(f"Released lock: {mp.current_process().name}")
                    pass

def  get_message_to_bot(result_list):
    main_message=""
    for exchanger in result_list:
        message=""
        for key,value in exchanger.items():
            message+=key
            for key1,value1 in value.items():
                message+="\n"
                message+=key1
                
                absence=""
                presence=""
                for element in value1:
                    if element[2]=="+":
                        presence+="\n"
                        presence+=element[1]
                    elif element[2]=="-":
                        absence+="\n"
                        absence+=element[1]
                
                message+="\n"
                message+="Отсутствие-\n"
                message+=absence
                
                message+="\n"
                message+="Присутствие-\n"
                message+=presence

                message+="\n"
        main_message+=message+"\n\n"
    return main_message

def create_result_structure(data):
    result_structure=list()
    for i in data:
        city=i[0]
        exchangers=i[1].split(", ")
        for j in exchangers:
            
            flag=-1
            for element in result_structure:
                if j in element:
                    flag=result_structure.index(element)
            
            if flag==-1:
                result_structure.append({j:{city:[]}})
            else:
                flag1=False
                for k in result_structure[flag][j]:
                    if k==city:
                        flag1=True
                if flag1==False:
                    result_structure[flag][j][city]=[]
    return result_structure

def convert_structure_to_shared(structure):
    manager = mp.Manager()
    result_structure_shared = manager.list()

    for item in structure:
        converted_item = manager.dict()

        for key, value in item.items():
            converted_value = manager.dict()

            for inner_key, inner_value in value.items():
                converted_inner_value = manager.list(inner_value)
                converted_value[inner_key] = converted_inner_value

            converted_item[key] = converted_value

        result_structure_shared.append(converted_item)

    return result_structure_shared                

def convert_structure_to_common(shared):
    common = []

    for item in shared:
        converted_item = {}

        for key, value in item.items():
            converted_value = {}

            for inner_key, inner_value in value.items():
                converted_inner_value = list(inner_value)
                converted_value[inner_key] = converted_inner_value

            converted_item[key] = converted_value

        common.append(converted_item)

    return common

def main():
    main_data,y=get_data_from_google_table() 
    
    result_structure=create_result_structure(main_data)
    
    result_structure_shared=convert_structure_to_shared(result_structure)

    #result_structure - незаполненная итоговая структура
    #result_structure_shared - незаполненная итоговая структура доступная для изменений внутри разных процессов
    
    manager = mp.Manager()
    lock = manager.Lock()
    process_list = []
    for element in main_data:
        p = mp.Process(target=get_formated_data, args=(element, result_structure_shared, lock,))
        process_list.append(p)
    for process in process_list:
        process.start()
    for process in process_list:
        process.join()
    #result_structure_shared - теперь заполненная итоговая структура доступная для изменений внутри разных процессов
    
    result_structure=convert_structure_to_common(result_structure_shared)
    print(result_structure)
    #result_structure - теперь заполненная итоговая структура
    
    print(get_message_to_bot(result_structure))

if __name__ == '__main__':
    t1 = time.time()
    main()
    t2 = time.time()
    print("Прошло за ", t2 - t1)
