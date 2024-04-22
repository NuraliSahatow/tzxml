from db import connect_to_db, create_table
from xml import process_xml
import time
def main():
    filename = "./elektronika_products_20240421_155417.xml"
    
    print("Подключения к базе данных...")
    conn = connect_to_db()
    
    print("Создание таблицы если оно еще не созданна...")
    create_table(conn)
    print(f"Выполняем файл: {filename}")
    process_xml(filename, conn)
    
    print("Закрываем подключения с базой данных...")
    conn.close()
    print("Готово.")

if __name__ == "__main__":
    main()
