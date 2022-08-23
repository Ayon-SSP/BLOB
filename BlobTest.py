"""
This article focus on connecting to a PostgreSQL database from Python.


https://www.geeksforgeeks.org/install-postgresql-on-windows/


To connect to a PostgreSQL database, we need to install psycopg2 library.

`
pip3 install psycopg2
`

psycopg2 is a Python module that allows us to connect to a PostgreSQL database.
we need to connect to a PostgreSQL database using psycopg2.connect() function.
where the attributes are:
    hostname = 'localhost'
    database = 'For_Practice'
    username = 'postgres'
    pwd = '321654'
    port_id = 5432
    
In case you don't know any of this connect() function attributes, you can Follow the below steps:


To connect to the above created database we use the connect () function.
Now to connect to the database we need to pass the attributes as arguments to the connect () function.


Create a cursor(ie,curr) object and call its execute() method to execute queries.
where execute() method is used to run a query that is passed as string.
`cur = conn.cursor()`


postgresql is a relational database management system (RDBMS) that is used to store data in a relational database.
Execute a query
At the end Don't forget to close the connection.




Storing a BLOB in a PostgreSQL Database using Python
Handling PostgreSQL BLOB data in Python
Retrieve Blob Datatype from Postgres with Python


The above mentioned article is a tutorial on how to store BLOB data in a PostgreSQL database.
BLOB is a binary large object (BLOB) is a data type that can store any binary data.



To store BLOB data in a PostgreSQL database, we need to use the Binary Large Object (BLOB) data type.
By using the Binary Large Object (BLOB) data type, we can store any binary data in a PostgreSQL database.

The different types of BLOB data that we can store in Database are:
    1. .png
    2. .jpg
    3. .gif
    4. .pdf
    5. .docx
    6. .xlsx
    7. .mp4 & .mp3 ...etc.

Above code is a simple example to store a BLOB data in a PostgreSQL database.
`
psycopg2.Binary(File_in_Bytes)
`

This article focus on 

` 
open('files\Anima.mp4', 'rb').read()
`

"""

import psycopg2

conn = None
try:
    # connect to the PostgreSQL server
    conn = psycopg2.connect(
        host = 'localhost',
        dbname = 'For_Practice',
        user = 'postgres',
        password = '321654',
        port = 5432
    )
    
    # Creating a cursor with name cur.
    cur = conn.cursor()
    
    # cur.execute('TRUNCATE TABLE BLOB_DataStore')
    
    
    # SQL query to insert data into the database.
    insert_script = '''
        INSERT INTO blob_datastore(s_no,file_name,blob_data) VALUES (%s,%s,%s);
    '''
    
    # open('File,'rb').read() is used to read the file.
    # where open(File,'rb').read() will return the binary data of the file.
    # psycopg2.Binary(File_in_Bytes) is used to convert the binary data to a BLOB data type.
    BLOB_1 = psycopg2.Binary(open('files\Anima.mp4', 'rb').read())       # Video
    BLOB_2 = psycopg2.Binary(open('files\Octa.jpg', 'rb').read())        # Image
    BLOB_3 = psycopg2.Binary(open('files\Type.gif', 'rb').read())        # GIF
    BLOB_4 = psycopg2.Binary(open('files\BlobNotes.pdf', 'rb').read())   # PDF
    
    # And Finally we pass the above mentioned values to the insert_script variable. 
    insert_values = [(1,'Anima.mp4',BLOB_1),(2,'Octa.jpg',BLOB_2),(3,'Type.gif',BLOB_3),(4,'BlobNotes.pdf',BLOB_4)]
    
    # The execute() method with the insert_script & insert_value as argument.
    for insert_value in insert_values:
        cur.execute(insert_script,insert_value) 
        print(insert_value[0],insert_value[1],"[Binary Data]" ,"row Inserted Successfully")
    
    
        
        
    # SQL query to fetch data from the database.
    cur.execute('SELECT * FROM BLOB_DataStore')
    
    # open(file,'wb').write() is used to write the binary data to the file.
    
    
    for row in cur.fetchall():
        BLOB = row[2]
        open("new"+row[1], 'wb').write(BLOB)
        print(row[0],row[1],"BLOB Data is saved in Current Directory")
    
    # Close the connection
    cur.close()
    
    
except(Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        # Commit the changes to the database
        conn.commit()