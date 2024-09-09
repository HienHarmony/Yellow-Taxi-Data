import psycopg2

# Thông tin kết nối đến cơ sở dữ liệu PostgreSQL
config = {
    'user': 'vanhien',
    'password': '1234',
    'host': 'localhost',
    'database': 'hdtoday_data'
}

# Kết nối đến cơ sở dữ liệu PostgreSQL
try:
    connection = psycopg2.connect(**config)
    cursor = connection.cursor()
    print("Kết nối thành công")
    cursor.execute("SELECT table_name  FROM information_schema.tables WHERE table_schema = 'public';")
    tables= cursor.fetchall()
    print("các bảng trong dữ liệu là :")
    for table in tables:
       print(table[0])
    cursor.close()
    connection.close()
except psycopg2.Error as error:
    print("Đã xảy ra lỗi khi thực hiện kết nối:", error)
