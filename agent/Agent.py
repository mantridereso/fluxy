__author__ = 'martin'

import time
from agent.Behaviour import *
from AbstractAgent import *
from message.Parser import *


class BaseAgentIdentity(AbstractAgentIdentity):

    class SendRequestBuilder():

        class SendRequestBuilder_2():

            class SendRequestBuilder_3():

                class TimeoutRunner(threading.Thread):

                    def __init__(self, agentIdentity, pattern, callback, seconds):
                        threading.Thread.__init__(self)
                        self._agentIdentity = agentIdentity
                        self._pattern = pattern
                        self._callback = callback
                        self._seconds = seconds

                    def run(self):
                        time.sleep(self._seconds)
                        if self._agentIdentity.getBehaviour().removeReactionPattern(self._pattern):
                            self._callback()


                def __init__(self,  agentIdentity, requestMessage, agentIdentityReceiver, seconds, timeoutCallback):
                    """

                    :param agentIdentity: AgentIdentity
                    :param requestMessage: RequestMessage
                    :param agentIdentityReceiver: AgentIdentity
                    """
                    assert isinstance(agentIdentityReceiver, AbstractAgentIdentity)
                    self._agentIdentity = agentIdentity
                    self._requestMessage = requestMessage
                    self._agentIdentityReceiver = agentIdentityReceiver
                    self._seconds = seconds
                    self._timeoutCallback = timeoutCallback

                def reactUponReply(self, reactionCallback):
                    requestMessage = self._requestMessage
                    requestMessage.setSender(self._agentIdentity.getSenderIdentity())
                    requestMessage.setReceiver(self._agentIdentityReceiver)
                    requestMessage.autoGenerateConversationId()
                    pattern = self._agentIdentity.getBehaviour().addOnReplyCallback(requestMessage, reactionCallback)
                    self._agentIdentity.getAgent().sendMessage(requestMessage)
                    self.TimeoutRunner(self._agentIdentity, pattern, self._timeoutCallback, self._seconds).start()

            def __init__(self,  agentIdentity, requestMessage, agentIdentityReceiver):
                """

                :param agentIdentity: AgentIdentity
                :param requestMessage: RequestMessage
                :param agentIdentityReceiver: AgentIdentity
                """
                assert isinstance(agentIdentityReceiver, AbstractAgentIdentity)
                self._agentIdentity = agentIdentity
                self._requestMessage = requestMessage
                self._agentIdentityReceiver = agentIdentityReceiver

            def reactUponReply(self, reactionCallback):
                requestMessage = self._requestMessage
                requestMessage.setSender(self._agentIdentity.getSenderIdentity())
                requestMessage.setReceiver(self._agentIdentityReceiver)
                requestMessage.autoGenerateConversationId()
                self._agentIdentity.getBehaviour().addOnReplyCallback(requestMessage, reactionCallback)
                self._agentIdentity.getAgent().sendMessage(requestMessage)

            def onTimeOut(self, seconds, timeoutCallback):
                return self.SendRequestBuilder_3(self._agentIdentity, self._requestMessage, self._agentIdentityReceiver, seconds, timeoutCallback)


        def __init__(self, agentIdentity, requestMessage):
            assert isinstance(agentIdentity, BaseAgentIdentity)
            assert isinstance(requestMessage, RequestMessage)
            self._agentIdentity = agentIdentity
            self._requestMessage = requestMessage

        def to(self, agentIdentityReceiver):
            """

            :param agentIdentityReceiver:
            :rtype: influx.AgentIdentity.SendRequestBuilder.SendRequestBuilder_2
            """
            return self.SendRequestBuilder_2(self._agentIdentity, self._requestMessage, agentIdentityReceiver)

    class SendReplyBuilder():

        def __init__(self, agentIdentity, requestMessage):
            assert isinstance(agentIdentity, BaseAgentIdentity)
            assert isinstance(requestMessage, RequestMessage)
            self._agentIdentity = agentIdentity
            self._requestMessage = requestMessage

        def sendMessage(self, replyMessage):
            agent = self._agentIdentity.getAgent()
            reply = replyMessage.inReplyTo(self._requestMessage)
            agent.sendMessage(reply)

    class SendInformMessageBuilder():

        def __init__(self, agentIdentity, informMessage):
            assert isinstance(informMessage, InformMessage)
            assert isinstance(agentIdentity, BaseAgentIdentity)
            self._informMessage = informMessage
            self._agentIdentity = agentIdentity

        def to(self, agentIdentityReceiver):
            informMessage = self._informMessage
            informMessage.setSender(self._agentIdentity.getSenderIdentity())
            informMessage.setReceiver(agentIdentityReceiver)
            self._agentIdentity.getAgent().sendMessage(informMessage)

    def __init__(self, agent, roleName, groupName):
        AbstractAgentIdentity.__init__(self, agent, roleName, groupName)
        assert isinstance(agent, BaseAgent)
        self._behaviour = Behaviour(self)

    def getBehaviour(self):
        """

        :rtype : Behaviour
        """
        return self._behaviour

    def sendRequest(self, requestMessage):
        """

        :rtype : influx.AgentIdentity.SendRequestBuilder
        """
        return self.SendRequestBuilder(self, requestMessage)

    def inReplyTo(self, requestMessage):
        """

        :rtype : influx.AgentIdentity.SendReplyBuilder
        """
        return self.SendReplyBuilder(self, requestMessage)

    def sendInformMessage(self, informMessage):
        return self.SendInformMessageBuilder(self, informMessage)

    def react(self, message):
        assert isinstance(message, CommunicationMessage)
        return self.getBehaviour().react(message)

    def getAgent(self):
        """

        :rtype : BaseAgent
        """
        return AbstractAgentIdentity.getAgent(self)

    def isApproved(self):
        raise NotImplemented

    def getSenderIdentity(self):
        return self


class AgentIdentity(BaseAgentIdentity):

    class RequestRoleBuilder():

        class RequestRoleBuilder_2():

            def __init__(self, agentIdentity, roleName, groupName):
                assert isinstance(agentIdentity, AgentIdentity)
                self._agentIdentity = agentIdentity
                self._roleName = roleName
                self._groupName = groupName
                self._callbackOnSuccess = None
                self._callbackOnDenied = None
                self._callbackOnGroupNotFound = None

            def _isComplete(self):
                if self._callbackOnSuccess is None:
                    return False
                if self._callbackOnDenied is None:
                    return False
                if self._callbackOnGroupNotFound is None:
                    return False
                return True

            def _fireOnComplete(self):
                if self._isComplete():
                    _success = self._callbackOnSuccess
                    _denied = self._callbackOnDenied
                    _groupNotFound = self._callbackOnGroupNotFound

                    _roleName = self._roleName
                    _groupName = self._groupName

                    def _requestRole(groupManagerIdentity, _callbackOnReply):
                        assert isinstance(groupManagerIdentity, AbstractAgentIdentity)
                        self._agentIdentity \
                            .sendRequest(RoleRequestMessage(_roleName)) \
                            .to(groupManagerIdentity) \
                            .reactUponReply(_callbackOnReply) #TODO AEHAEM!!!

                    def _flipGroupManagerIdentity(memberIdentityInALL):

                        assert isinstance(memberIdentityInALL, AbstractAgentIdentity)
                        assert memberIdentityInALL.getGroup() == ALLGroup.NAME
                        return AbstractAgentIdentity(memberIdentityInALL.getAgent(), Group.ROLE_MANAGER, _groupName)

                    def _callback_1(message):
                        if isinstance(message, GroupManagerFoundReplyMessage):
                            _requestRole(_flipGroupManagerIdentity(message.getGroupManager()), _callback_2)
                        if isinstance(message, GroupManagerNotFoundReplyMessage):
                            _groupNotFound(message)

                    def _callback_2(message):
                        if isinstance(message, RoleRequestSuccessMessage):
                            identity = self._agentIdentity.getAgent().inRole(self._roleName).withinGroup(self._groupName)
                            identity._roleAddress = message.getRoleAddress()
                            identity.approve()
                            _success(message)
                        elif isinstance(message, RoleRequestFailedMessage):
                            _denied(message)

                    if self._groupName == ALLGroup.NAME:
                        _requestRole(getMTLImplementation().getPlatformManager(), _callback_2)
                    else:
                        self._agentIdentity \
                            .sendRequest(GetGroupManagerAddressRequestMessage(self._groupName)) \
                            .to(getMTLImplementation().getPlatformManager()) \
                            .reactUponReply(_callback_1)

            def onSuccess(self, callback):
                """

                :rtype : influx.AgentIdentity.RequestRoleBuilder.RequestRoleBuilder_2
                """
                assert(callback is None or callable(callback))
                if not self._isComplete():
                    self._callbackOnSuccess = callback
                    self._fireOnComplete()
                return self

            def onDenied(self, callback):
                """

                :rtype : influx.AgentIdentity.RequestRoleBuilder.RequestRoleBuilder_2
                """
                assert(callback is None or callable(callback))
                if not self._isComplete():
                    self._callbackOnDenied = callback
                    self._fireOnComplete()
                return self

            def onGroupNotFound(self, callback):
                """

                :rtype : influx.AgentIdentity.RequestRoleBuilder.RequestRoleBuilder_2
                """
                assert(callback is None or callable(callback))
                if not self._isComplete():
                    self._callbackOnGroupNotFound = callback
                    self._fireOnComplete()
                return self


        def __init__(self, agentIdentity, roleName):
            self._agentIdentity = agentIdentity
            self._roleName = roleName

        def withinGroup(self, groupName):
            """

            :rtype : influx.AgentIdentity.RequestRoleBuilder.RequestRoleBuilder_2
            """
            return self.RequestRoleBuilder_2(self._agentIdentity, self._roleName, groupName)

    class CreateGroupBuilder():

        def __init__(self, agentIdentity, group):
            assert isinstance(group, Group)
            assert isinstance(agentIdentity, AgentIdentity)
            self._agentIdentity = agentIdentity
            self._group = group
            self._onSuccess = None
            self._onFail = None

        def _isComplete(self):
            return self._onFail is not None and self._onSuccess is not None

        def _fire(self):

            _group = self._group
            _onSuccess = self._onSuccess
            _onFail = self._onFail

            class Runner(threading.Thread):
                def run(self):
                    groupManager = GroupManager(_group)
                    groupManager.bindToMTLAgent(getMTLImplementation().getMTLAgent("group_manager"+_group.getName()))
                    groupManager.enterPlatform(_onSuccess, _onFail)

            Runner().start()

        def _fireOnComplete(self):
            if self._isComplete():
                self._fire()

        def onSuccess(self, onSuccessCallback):
            assert callable(onSuccessCallback)
            self._onSuccess = onSuccessCallback
            self._fireOnComplete()
            return self

        def onFail(self, onFailCallback):
            assert callable(onFailCallback)
            self._onFail = onFailCallback
            self._fireOnComplete()
            return self

    class WithAgentsInRoleDoBuilder():

        def __init__(self, agentIdentity, roleName):
            assert isinstance(agentIdentity, AgentIdentity)
            self._agentIdentity = agentIdentity
            self._roleName = roleName

        def do(self, callBackForAgentsList):
            assert callable(callBackForAgentsList)

            def _flipGroupManagerIdentity(memberIdentityInALL):

                assert isinstance(memberIdentityInALL, AbstractAgentIdentity)
                assert memberIdentityInALL.getGroup() == ALLGroup.NAME
                return AbstractAgentIdentity(memberIdentityInALL.getAgent(), Group.ROLE_MANAGER, self._agentIdentity.getGroup())

            def _callback_2(message):
                assert isinstance(message, FoundAgentsWithRoleMessage)
                callBackForAgentsList(message.getAgents())

            def _callback_1(message):

                if isinstance(message, GroupManagerFoundReplyMessage):
                    groupManager = _flipGroupManagerIdentity(message.getGroupManager())
                    self._agentIdentity \
                        .sendRequest(FindAgentsWithRoleMessage(self._roleName)) \
                        .to(groupManager) \
                        .reactUponReply(_callback_2)
                elif isinstance(message, GroupManagerNotFoundReplyMessage):
                    pass # TODO just for the moment, raise some Exception

            self._agentIdentity \
                .sendRequest(GetGroupManagerAddressRequestMessage(self._agentIdentity.getGroup())) \
                .to(getMTLImplementation().getPlatformManager()) \
                .reactUponReply(_callback_1)

    def __init__(self, agent, roleName, groupName):
        BaseAgentIdentity.__init__(self, agent, roleName, groupName)
        assert isinstance(agent, Agent)
        self._roleAddress = None
        self._approved = False
        self._behaviour = Behaviour(self)

    def isApproved(self):
        return self._approved

    def approve(self):
        if not self._approved:
            self.getBehaviour().startActing()
            self._approved = True

    def revokeApproval(self):
        pass

    def requestRole(self, roleName):
        """

        :rtype : influx.AgentIdentity.RequestRoleBuilder
        """
        return self.RequestRoleBuilder(self, roleName)

    def createGroup(self, group):
        return self.CreateGroupBuilder(self, group)

    def withAgentsInRole(self, roleName):
        return self.WithAgentsInRoleDoBuilder(self, roleName)

    def getSenderIdentity(self):
        roleAddress = self._roleAddress
        if isinstance(roleAddress, ParticularAgent):
            return roleAddress.inRole(self.getRole()).withinGroup(self.getGroup())
        else:
            return self


class BaseAgent(ParticularAgent):

    class InvalidRawMessageType(Exception):
        def __init__(self, rawMessageInput):
            self._rawMessageInput = rawMessageInput

        def getRawMessageInput(self):
            return self._rawMessageInput

    class _IdentityBuilderAgent():
        def __init__(self, agent, roleName):
            """

            :param agent: BaseAgent
            :param roleName:
            """
            self._agent = agent
            self._roleName = roleName

        def withinGroup(self, groupNameOrGroup):
            """

            :rtype : AgentIdentity
            """
            groupName = groupNameOrGroup
            if isinstance(groupNameOrGroup, Group):
                groupName = groupNameOrGroup.getName()
            #assert isinstance(groupName, basestring)
            return self._agent.getIdentity(groupName, self._roleName)

    def __init__(self):
        ParticularAgent.__init__(self)
        self._messageParser = MessageParser.getInstance()


    def getIdentity(self, groupName, roleName):
        """

        :rtype : BaseAgentIdentity
        """
        raise NotImplemented

    def inRole(self, roleName):
        """

        :rtype : Agent._IdentityBuilderAgent
        """
        return self._IdentityBuilderAgent(self, roleName)

    def sendMessage(self, message):
        raise NotImplemented

    def receiveMessage(self, rawMessage):
        message = self._messageParser.parseMessage(rawMessage)
        if isinstance(message, PingMessage):
            self.sendMessage(PongMessage().inReplyTo(message))
        else:
            receiver = message.getReceiver()
            groupName = receiver.getGroup()
            roleName = receiver.getRole()
            return self.getIdentity(groupName, roleName).react(message)

    def getAddress(self):

        """

        :rtype : AgentAddress
        """
        raise NotImplemented


class Agent(BaseAgent):

    class MessageConsumer(threading.Thread):

        def __init__(self, agent):
            threading.Thread.__init__(self)
            assert isinstance(agent, Agent)
            self._agent = agent

        def run(self):
            while True:
                queueLen = len(self._agent._queue)
                while queueLen == 0:
                    time.sleep(0.02)
                    queueLen = len(self._agent._queue)
                try:
                    for i in range(queueLen):
                        messageString = self._agent._queue.popleft()
                        self._agent.receiveMessage(json.loads(messageString))
                except IndexError:
                    pass


    def __init__(self):
        BaseAgent.__init__(self)
        self._MTLAgent = None
        self._authenticatedForALL = False
        self._identities = dict()
        self._queue = collections.deque()
        self.MessageConsumer(self).start()

    def getIdentity(self, groupName, roleName):
        """

        :rtype : AgentIdentity
        """
        if not groupName in self._identities:
            self._identities[groupName] = dict()
        if not roleName in self._identities[groupName]:
            self._identities[groupName][roleName] = AgentIdentity(self, roleName, groupName)
        return self._identities[groupName][roleName]

    def bindToMTLAgent(self, mtlAgent):
        """

        :param MTLAgent: MTLAgent
        :raise self.NotAnMTLAgent:
        """
        if isinstance(mtlAgent, MTLAgent):
            self._MTLAgent = mtlAgent
            self._MTLAgent.bindToFluxyAgent(self)
        else:
            raise NotAnMTLAgent()

    def sendMessage(self, message):
        if self._MTLAgent:
            self._MTLAgent.sendMessage(message)
        else:
            raise NoMTLLayerBinding()

    def process(self):
        """

        can be overridden
        """
        while True:
            time.sleep(1.5)

    def inBaseRole(self):
        return self.inRole(ALLGroup.ROLE_ACTOR).withinGroup(ALLGroup.NAME)

    def onEnterPlatform(self):
        pass

    def enterPlatform(self, onSuccessCallback=None, onDeniedCallback=None, onPlatformDownCallback=None):

        self._MTLAgent.start()

        candidateSelf = self.inRole(ALLGroup.ROLE_CANDIDATE).withinGroup(ALLGroup.NAME)

        baseSelf = self.inBaseRole()

        def __success(message):
            self.onEnterPlatform()

        def __denied(message):
            pass

        def __platformDown(message):
            pass

        candidateSelf \
            .requestRole(baseSelf.getRole()) \
            .withinGroup(baseSelf.getGroup()) \
            .onDenied(__denied if onDeniedCallback is None else onDeniedCallback) \
            .onGroupNotFound(__platformDown if onPlatformDownCallback is None else onPlatformDownCallback) \
            .onSuccess(__success if onSuccessCallback is None else onSuccessCallback)

    def getAddress(self):
        if self._MTLAgent:
            ret = AgentAddress(self._MTLAgent.getAddress())
            return ret
        else:
            return None

    def enqueueMessage(self, stringMessage):
        self._queue.append(stringMessage)


class GroupProxyAgent(Agent):

    ROLE_GROUP_PROXY = "__proxy__"

    def __init__(self, group, realParticularAgent):
        Agent.__init__(self)
        assert isinstance(group, Group)
        assert isinstance(realParticularAgent, ParticularAgent)
        self._group = group
        self._realAgentAddress = realParticularAgent.getAddress().export()

    def registerRole(self, role):
        identity = self.inRole(role).withinGroup(self._group.getName())
        return identity

    def receiveMessage(self, rawMessage):
        if rawMessage[AbstractMessage.KEY_HEADER][AbstractMessage.AbstractHeader.KEY_MESSAGE_TYPE] == PingMessage.MESSAGE_TYPE:
            pingMessage = MessageParser.getInstance().parseMessage(rawMessage)
            self.sendMessage(PongMessage().inReplyTo(pingMessage))
        else:
            rawMessage[AbstractMessage.KEY_HEADER][AbstractMessage.AbstractHeader.KEY_RECEIVER][AbstractAgentIdentity.KEY_ACTOR] = self._realAgentAddress
            self.sendMessage(rawMessage)


class Group():

    ROLE_MANAGER = "__manager__"

    class RoleRequestDenied(Exception):

        def __init__(self, reason="none of your business"):
            self._reason = reason

        def getReason(self):
            return self._reason

    def __init__(self, groupName):
        self._name = groupName
        self._agentsByRoles = dict()
        self._manager = None

    def getName(self):
        return self._name

    def requestRole(self, agentIdentity, roleName):
        assert isinstance(agentIdentity, AbstractAgentIdentity)

        if roleName == self.ROLE_MANAGER:
            if self._manager is None:
                self._manager = AbstractAgentIdentity(agentIdentity.getAgent(), self.ROLE_MANAGER, self.getName())
                return
            else:
                raise self.RoleRequestDenied()

        if not ((roleName in self._agentsByRoles)
                and (str(agentIdentity.getAgent()) in self._agentsByRoles[roleName])):

            if not self.acceptRoleRequest(agentIdentity, roleName):
                raise self.RoleRequestDenied()

            if not roleName in self._agentsByRoles:
                self._agentsByRoles[roleName] = dict()
            self._agentsByRoles[roleName][str(agentIdentity.getAgent())] = AbstractAgentIdentity(agentIdentity.getAgent(), roleName, self.getName())

    def getAgentsWithRole(self, roleName):
        if not roleName in self._agentsByRoles:
            return None
        return self._agentsByRoles[roleName].values()

    def acceptRoleRequest(self, agentIdentity, roleName):
        assert isinstance(agentIdentity, AbstractAgentIdentity)

        return True

    def getAgents(self):
        agents = dict()
        for role, agentsWithRole in self._agentsByRoles.items():
            if not role == self.ROLE_MANAGER:
                agents.update(agentsWithRole)
        return agents.viewvalues()

    def unregisterAgent(self, agentIdentity):
        assert isinstance(agentIdentity, AbstractAgentIdentity)
        agentStr = str(agentIdentity.getAgent())
        if agentIdentity.getRole() in self._agentsByRoles and agentStr in self._agentsByRoles[agentIdentity.getRole()]:
            del self._agentsByRoles[agentIdentity.getRole()][agentStr]
            if len(self._agentsByRoles[agentIdentity.getRole()]) == 0:
                del self._agentsByRoles[agentIdentity.getRole()]

class AbstractGroupManager(Agent):

    class RequestRoleReactionPattern(ReactionPattern):

        def acceptMessage(self, message):
            return isinstance(message, RoleRequestMessage)

        def receiveMessage(self, message):
            groupManager = self.getAgentIdentity().getAgent()
            assert isinstance(groupManager, AbstractGroupManager)
            assert isinstance(message, RoleRequestMessage)
            try:
                groupManager.getGroup().requestRole(message.getSender(), message.getRoleName())
                groupManager.onRoleRequest(message.getSender(), message.getRoleName())
                self.getAgentIdentity() \
                    .inReplyTo(message) \
                    .sendMessage(RoleRequestSuccessMessage(self.getAgentIdentity().getAgent()._getRoleAddress(message.getSender().getAgent(),message.getRoleName())))
            except Group.RoleRequestDenied as denied:
                self.getAgentIdentity().inReplyTo(message).sendMessage(RoleRequestFailedMessage(denied.getReason()))
            except Exception as e:
                print("exception in AbstractGroupManager.receiveMessage(RequestRoleReactionPattern)")
                print(e.__class__.__name__)
                print(e.message)
                raise e

    class PingActionPattern(ActionPattern):

        @staticmethod
        def lostAgent(group, agentIdentity):
            def _lost():
                group.unregisterAgent(agentIdentity)
            return _lost

        def __init__(self, agentIdentity, group):
            ActionPattern.__init__(self, agentIdentity)
            self._group = group

        def process(self):
            def _ok(message):
                pass
            while True:
                time.sleep(10)
                agents = self._group.getAgents()

                for agent in agents:
                    self._agentIdentity\
                        .sendRequest(PingMessage())\
                        .to(agent)\
                        .onTimeOut(7,self.lostAgent(self._group, agent))\
                        .reactUponReply(_ok)


    def __init__(self, group):
        """

        :param group: Group
        """
        Agent.__init__(self)
        assert isinstance(group, Group)
        self._group = group

        groupManagerSelf = self.inManagerRole()
        groupManagerSelf.getBehaviour().addReactionPattern(self.RequestRoleReactionPattern(groupManagerSelf))

    def inManagerRole(self):
        """

        :rtype : AgentIdentity
        """
        return self.inRole(Group.ROLE_MANAGER).withinGroup(self.getGroup())

    def getGroup(self):
        return self._group

    def onRoleRequest(self, targetAgentIdentity, roleName):
        """

        :param targetAgentIdentity:
        :param roleName:
        """
        assert isinstance(targetAgentIdentity, AbstractAgentIdentity)
        pass

    def _getRoleAddress(self, forAgent, roleName):
        assert isinstance(forAgent, ParticularAgent)
        return forAgent


class GroupManager(AbstractGroupManager):

    class FindAgentsWithRoleReactionPattern(ReactionPattern):

        def acceptMessage(self, message):
            return isinstance(message, FindAgentsWithRoleMessage)

        def receiveMessage(self, message):
            assert isinstance(message, FindAgentsWithRoleMessage)
            groupManager = self.getAgentIdentity().getAgent()
            assert isinstance(groupManager, GroupManager)
            group = groupManager.getGroup()
            agentsWithRole = group.getAgentsWithRole(message.getRoleName())
            if agentsWithRole is None:
                agentsWithRole = []
            proxyAgentsWithRoles = [groupManager.convertAgentIdentityToProxyIdentity(x) for x in list(agentsWithRole)]

            reply = FoundAgentsWithRoleMessage(proxyAgentsWithRoles).inReplyTo(message)
            self.getAgentIdentity().inReplyTo(message).sendMessage(reply)

    def __init__(self, group):
        AbstractGroupManager.__init__(self, group)
        self._proxyAgents = dict()
        self._groupProxyAgents = dict()
        self.inManagerRole().getBehaviour().addReactionPattern(self.FindAgentsWithRoleReactionPattern(self.inManagerRole()))

    def inBaseRole(self):
        return self.inRole(ALLGroup.ROLE_GROUP_MANAGER_PREFIX+self.getGroup().getName()).withinGroup(ALLGroup.NAME)

    def convertAgentIdentityToProxyIdentity(self, agentIdentity):
        assert isinstance(agentIdentity, AbstractAgentIdentity)

        groupProxyAgent, roles = self._groupProxyAgents[agentIdentity.getAgent().getHashKey()]

        return roles[agentIdentity.getRole()]

    def enterPlatform(self, onSuccessCallback=None, onDeniedCallback=None, onPlatformDownCallback=None):
        def _callbackSuccess(message):
            self._group.requestRole(self.inBaseRole(), Group.ROLE_MANAGER)
            self.inManagerRole().approve()
            onSuccessCallback(message)
        AbstractGroupManager.enterPlatform(self,_callbackSuccess, onDeniedCallback, onPlatformDownCallback)

    def onRoleRequest(self, targetAgentIdentity, roleName):
        assert isinstance(targetAgentIdentity, AbstractAgentIdentity)

    def _getRoleAddress(self, forAgent, roleName):
        if not (forAgent.getHashKey() in self._groupProxyAgents):
            #start group proxy agent
            groupProxyAgent = GroupProxyAgent(self.getGroup(), forAgent)
            groupProxyAgent.bindToMTLAgent(getMTLImplementation().getMTLAgent("__proxy__"+forAgent.getHashKey()))
            groupProxyAgent.enterPlatform()
            self._groupProxyAgents[forAgent.getHashKey()] = groupProxyAgent, dict()

        groupProxyAgent, roles = self._groupProxyAgents[forAgent.getHashKey()]

        if not roleName in roles:
            roles[roleName] = groupProxyAgent.registerRole(roleName)

        return groupProxyAgent



class ALLGroup(Group):

    NAME = "__FLUXY_ALL__"
    ROLE_ACTOR = "__fluxy_agent__"
    ROLE_GROUP_MANAGER_PREFIX = "__fluxy_group_manager__"
    ROLE_CANDIDATE = "__candidate__"

    def __init__(self):
        Group.__init__(self, self.NAME)

    def acceptRoleRequest(self, agentIdentity, roleName):
        if not Group.acceptRoleRequest(self, agentIdentity, roleName):
            return False
        role = str(roleName)

        if role == self.ROLE_ACTOR:
            return True

        if role.startswith(self.ROLE_GROUP_MANAGER_PREFIX):
            res = self.getAgentsWithRole(role) is None
            return res

        return False


class FluxyManager(AbstractGroupManager):

    class GetGroupManagerAddressReactionPattern(ReactionPattern):
        def acceptMessage(self, message):
            return isinstance(message, GetGroupManagerAddressRequestMessage)

        def receiveMessage(self, message):
            assert isinstance(message, GetGroupManagerAddressRequestMessage)
            pfManager = self.getAgentIdentity().getAgent()
            assert isinstance(pfManager, FluxyManager)
            groupManagers = pfManager.getGroup().getAgentsWithRole(ALLGroup.ROLE_GROUP_MANAGER_PREFIX+message.getGroupName())
            if groupManagers is None:
                self.getAgentIdentity().inReplyTo(message).sendMessage(GroupManagerNotFoundReplyMessage())
            else:
                self.getAgentIdentity().inReplyTo(message).sendMessage(GroupManagerFoundReplyMessage(list(groupManagers)[0]))

    def __init__(self):
        AbstractGroupManager.__init__(self, ALLGroup())
        groupManagerSelf = self.inManagerRole()
        groupManagerSelf.getBehaviour().addReactionPattern(self.GetGroupManagerAddressReactionPattern(groupManagerSelf))
        groupManagerSelf.getBehaviour().addActionPattern(self.PingActionPattern(groupManagerSelf, self._group))


    def startPlatform(self):
        self._MTLAgent.start()

        groupManagerSelf = self.inManagerRole()

        virtualMessage = RoleRequestMessage(groupManagerSelf)
        virtualMessage.setSender(groupManagerSelf)
        virtualMessage.setReceiver(groupManagerSelf)

        self.receiveMessage(virtualMessage.export())

        groupManagerSelf.approve()

    @staticmethod
    def platformManagerRoleName():
        return ALLGroup.ROLE_GROUP_MANAGER_PREFIX+ALLGroup.NAME

