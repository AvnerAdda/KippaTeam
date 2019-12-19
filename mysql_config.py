create_authors = '''CREATE TABLE Toward_DataScience.authors (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            name VARCHAR(100) NOT NULL UNIQUE,
                            member_since DATETIME NULL DEFAULT NULL,
                            description VARCHAR(300) NULL DEFAULT NULL,
                            following INT NULL DEFAULT NULL,
                            followers INT NULL DEFAULT NULL,
                            social_media TINYINT(1) NULL DEFAULT NULL)
                            ENGINE=INNODB'''

create_articles = '''CREATE TABLE Toward_DataScience.articles (
                            id_article INT AUTO_INCREMENT PRIMARY KEY,
                            title VARCHAR(100) NULL DEFAULT NULL UNIQUE,
                            subtitle VARCHAR(500) NULL DEFAULT NULL ,
                            id_author INT NULL DEFAULT NULL,
                            date DATETIME NULL DEFAULT NULL,
                            read_time INT NULL DEFAULT NULL,
                            is_Premium TINYINT(1) NULL DEFAULT NULL,
                            claps INT NULL DEFAULT NULL,
                            response INT NULL DEFAULT NULL,
                            tags TEXT NULL DEFAULT NULL,
                            CONSTRAINT fk_author
                            FOREIGN KEY (id_author) 
                                REFERENCES Toward_DataScience.authors(id))
                            ENGINE=INNODB'''

add_trend = '''ALTER TABLE Toward_DataScience.articles ADD total_trend INT'''

select_trend = '''SELECT id_article, date, tags, total_trend FROM Toward_DataScience.articles;'''

update_trend = '''UPDATE Toward_DataScience.articles 
    SET total_trend = (%s) 
    WHERE id_article = (%s);'''

insert_mysql_author = """INSERT IGNORE INTO Toward_DataScience.authors (name) VALUES (%s)"""

check_if_author = """SELECT name FROM Toward_DataScience.authors WHERE name=(%s)"""

update_mysql_author = """UPDATE Toward_DataScience.authors 
    SET member_since = (%s), description = (%s), following = (%s), followers= (%s),
    social_media= (%s)
    WHERE name = (%s);"""

insert_mysql_article = """INSERT INTO Toward_DataScience.articles (title ,subtitle ,\
     id_author, date , read_time , is_Premium, claps, response, tags) 
     VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s)"""

select_id = """SELECT id FROM Toward_DataScience.authors WHERE name = %s"""
