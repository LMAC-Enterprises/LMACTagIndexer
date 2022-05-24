import mysql.connector

from mysql.connector import MySQLConnection


class TagsHandler:
    _mysqlDb: MySQLConnection
    _tagHitMap: dict

    def __init__(self, databaseUser: str, databasePassword: str, databaseName: str, databaseServerHost: str):
        self._mysqlDb = mysql.connector.connect(
            host=databaseServerHost,
            user=databaseUser,
            password=databasePassword,
            database=databaseName
        )
        self._tagHitMap = {}

    def updateTags(self, tags: list):
        for tag in tags:
            if tag in self._tagHitMap:
                self._tagHitMap[tag] += 1
            else:
                self._tagHitMap[tag] = 1

    def commitChanges(self):
        cursor = self._mysqlDb.cursor()

        for tagName in self._tagHitMap:
            cursor.execute(
                'INSERT INTO LMACGalleryTags (tagText, hits) VALUES("{tag}", {hits}) ON DUPLICATE KEY UPDATE tagText="{tag}", hits=hits+{hits}'.format(
                    tag=tagName,
                    hits=self._tagHitMap[tagName]
                )
            )

        self._mysqlDb.commit()
