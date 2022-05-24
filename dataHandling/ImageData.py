import re
import mysql.connector

from re import Pattern
from mysql.connector import MySQLConnection
from services.Registry import RegistryHandler


class ImageEntity:
    MIN_TAG_LENGTH = 3
    MAX_TAG_LENGTH = 32

    _imageId: int
    _tags: list

    _tagRegex = r'[a-zA-Z0-9]+'
    _tagCompiledRegex: Pattern

    def __init__(self, imageId: int, tags: str):
        self._imageId = imageId
        self._tagCompiledRegex = re.compile(self._tagRegex)
        self._tags = self._explodeTags(tags)

    def _explodeTags(self, tags: str) -> list:
        matches = self._tagCompiledRegex.findall(tags)
        filteredMatches = []
        for match in matches:
            matchLen = len(match)
            if matchLen < ImageEntity.MIN_TAG_LENGTH:
                continue
            if matchLen > ImageEntity.MAX_TAG_LENGTH:
                continue

            filteredMatches.append(match.lower())

        return filteredMatches

    @property
    def tags(self) -> list:
        return self._tags

    @property
    def imageId(self) -> int:
        return self._imageId

    def __str__(self):
        return '{imageId} - {tags}'.format(imageId=self.imageId, tags=', '.join(self._tags))


class ImagesHandler:

    _mysqlDb: MySQLConnection
    _maxImageBunchSize: int

    def __init__(self, databaseUser: str, databasePassword: str, databaseName: str, databaseServerHost: str, maxImageBunchSize: int):
        self._mysqlDb = mysql.connector.connect(
            host=databaseServerHost,
            user=databaseUser,
            password=databasePassword,
            database=databaseName
        )
        self._registry = RegistryHandler()
        self._lastIndexedImageId = self._registry.getProperty(ImagesHandler.__name__, 'lastIndexedImageId', 0)
        self._maxImageBunchSize = maxImageBunchSize

    def fetchNextBunchOfImages(self) -> list:
        cursor = self._mysqlDb.cursor()

        cursor.execute(
            'SELECT imageid, tags FROM images WHERE imageId>{lastIndexedImageId} ORDER BY imageid ASC LIMIT {imageBunchSize}'.format(
                lastIndexedImageId=self._lastIndexedImageId,
                imageBunchSize=self._maxImageBunchSize
            )
        )

        fetchResult = cursor.fetchall()
        images = []
        highestImageId = self._lastIndexedImageId
        for imageRow in fetchResult:
            imageId = int(imageRow[0])
            tags = imageRow[1]
            images.append(
                ImageEntity(imageId, tags)
            )
            if highestImageId < imageId:
                highestImageId = imageId

        self._registry.setProperty(ImagesHandler.__name__, 'lastIndexedImageId', highestImageId)
        self._lastIndexedImageId = highestImageId

        return images
