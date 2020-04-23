import mysql.connector

mydb = mysql.connector.connect(host="localhost",
                              user="root",
                              passwd="",
                              database="schema1",
                              use_pure=False)
mycursor = mydb.cursor()
mycursor.execute("select data "
                 "from twitter_table_test "
                 "where twitter_table_test.created_at between '2020-03-15 21:20:00' and '2020-03-15 21:40:00' ")
result= mycursor.fetchone()

print(result)
