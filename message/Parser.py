__author__ = 'martin'

from Messages import *


class MessageParser():

    instance = None

    @staticmethod
    def getInstance():
        """

        :rtype : MessageParser
        """
        if MessageParser.instance is None:
            return MessageParser()
        else:
            return MessageParser.instance

    class UnknownMessageType(Exception):
        def __init__(self, messageType):
            self._messageType = messageType

        def getUnknownMessageTypeFound(self):
            return self._messageType

    class InvalidMessageStructure(Exception):
        def __init__(self, message):
            self._message = message


        def getInvalidMessage(self):
            return self._message

    _typeMap = {
        RequestMessage.MESSAGE_TYPE: RequestMessage,
        RequestReplyMessage.MESSAGE_TYPE: RequestReplyMessage,
        InformMessage.MESSAGE_TYPE: InformMessage,
        PingMessage.MESSAGE_TYPE: PingMessage,
        PongMessage.MESSAGE_TYPE: PongMessage,
        RoleRequestMessage.MESSAGE_TYPE: RoleRequestMessage,
        RoleRequestSuccessMessage.MESSAGE_TYPE: RoleRequestSuccessMessage,
        RoleRequestFailedMessage.MESSAGE_TYPE: RoleRequestFailedMessage,
        GetGroupManagerAddressRequestMessage.MESSAGE_TYPE: GetGroupManagerAddressRequestMessage,
        GroupManagerFoundReplyMessage.MESSAGE_TYPE: GroupManagerFoundReplyMessage,
        GroupManagerNotFoundReplyMessage.MESSAGE_TYPE: GroupManagerNotFoundReplyMessage,
        FindAgentsWithRoleMessage.MESSAGE_TYPE: FindAgentsWithRoleMessage,
        FoundAgentsWithRoleMessage.MESSAGE_TYPE: FoundAgentsWithRoleMessage
    }

    def __init__(self):
        MessageParser.instance = self
        self.typeMap = MessageParser._typeMap

    def registerType(self, messageClass):
        assert issubclass(messageClass, CommunicationMessage)
        self.typeMap[messageClass.MESSAGE_TYPE] = messageClass

    def parseMessage(self, rawMessage):
        """

        :rtype : CommunicationMessage
        """

        if not ((CommunicationMessage.KEY_HEADER in rawMessage) and (CommunicationMessage.KEY_PAYLOAD in rawMessage)):
            raise self.InvalidMessageStructure(rawMessage)

        rawHeader = rawMessage[CommunicationMessage.KEY_HEADER]
        assert AbstractMessage.AbstractHeader.KEY_MESSAGE_TYPE in rawHeader

        header = CommunicationMessage.Header.parse(rawHeader)

        messageType = header.getMessageType()

        if messageType in self.typeMap:

            messageClass = self.typeMap[messageType]
            assert issubclass(messageClass, AbstractMessage)
            message = messageClass()
            message.setHeader(header)
            message.parsePayload(rawMessage[AbstractMessage.KEY_PAYLOAD])

            #if not message.isWellFormed():
            #    raise self.InvalidMessageStructure(rawMessage)

            return message

        else:
            raise self.UnknownMessageType(messageType)
