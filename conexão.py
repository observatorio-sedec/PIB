import psycopg2
conexao = psycopg2.connect(database = "observatorio_db",  
                        user = "sedec", 
                        host= '10.43.88.10',
                        password = "j23b7d7a5smw",
                        port = 5502)