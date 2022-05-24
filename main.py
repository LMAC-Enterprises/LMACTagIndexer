import sys

from Configuration import Configuration
from dataHandling.ImageData import ImagesHandler
from dataHandling.TagsData import TagsHandler
from services.Registry import RegistryHandler


class Main:
    EXITCODE_OK = 0
    EXITCODE_ERROR = 1

    def __init__(self):
        exit(self._main(sys.argv))

    def _main(self, arguments: list):
        imagesHandler = ImagesHandler(
            Configuration.databaseUser,
            Configuration.databasePassword,
            Configuration.databaseName,
            Configuration.databaseServerHost,
            Configuration.maxImageBunchSizeToProcessPerSession
        )
        tagsHandler = TagsHandler(
            Configuration.databaseUser,
            Configuration.databasePassword,
            Configuration.databaseName,
            Configuration.databaseServerHost
        )
        registryHandler = RegistryHandler()

        bunchOfImages = imagesHandler.fetchNextBunchOfImages()
        for image in bunchOfImages:
            tagsHandler.updateTags(image.tags)

        tagsHandler.commitChanges()
        registryHandler.saveAll()

        return Main.EXITCODE_OK


if __name__ == '__main__':
    Main()
