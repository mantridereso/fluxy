import collections

__author__ = 'martin'

import threading

from message.Messages import *

from WorkerThreads import ThreadPool


class Behaviour():

    MAX_PATTERNS = 30000

    class TooManySimultaneousPatterns(Exception):
        pass

    def __init__(self, agentIdentity):
        self._agentIdentity = agentIdentity
        self._actionPatterns = dict()
        self._reactionPatterns = dict()
        self._onReplyReactionPatterns = dict()
        self._idPool = list(range(self.MAX_PATTERNS))
        self._workerPool = ThreadPool.getInstance()

    def startActing(self):
        for actionPattern in list(self._actionPatterns.viewvalues()):
            actionPattern.start()

    def addActionPattern(self, actionPattern):
        assert(isinstance(actionPattern, ActionPattern))
        patternId = self.requestPatternId()
        actionPattern.setId(patternId)
        self._actionPatterns[patternId] = actionPattern
        if self._agentIdentity.isApproved():
            actionPattern.start()

    def removeActionPattern(self, actionPattern):
        assert(isinstance(actionPattern, ActionPattern))
        patternId = actionPattern.getId()
        del self._actionPatterns[patternId]
        self.releasePatternId(patternId)

    def addReactionPattern(self, reactionPattern):
        assert(isinstance(reactionPattern, ReactionPattern))
        patternId = self.requestPatternId()
        reactionPattern.setId(patternId)
        self._reactionPatterns[patternId] = reactionPattern
        if self._agentIdentity.isApproved():
            reactionPattern.start()

    def addOnReplyCallback(self, requestMessage, callback):
        assert callable(callback)
        patternId = self.requestPatternId()
        conversationId = requestMessage.getConversationId()
        if not conversationId in self._onReplyReactionPatterns:
            self._onReplyReactionPatterns[conversationId] = dict()
        pattern = conversationId, patternId, callback
        try:
            self._onReplyReactionPatterns[conversationId][patternId] = pattern
        except KeyError:
            pass
        return pattern

    def removeReactionPattern(self, reactionPattern):
        conversationId, patternId, callback = reactionPattern
        if conversationId in self._onReplyReactionPatterns and patternId in self._onReplyReactionPatterns[conversationId]:
            del self._onReplyReactionPatterns[conversationId][patternId]
            if len(self._onReplyReactionPatterns[conversationId]) == 0:
                del self._onReplyReactionPatterns[conversationId]

            self.releasePatternId(patternId)
            return True
        else:
            return False

    def requestPatternId(self):
        patternId = self._idPool.pop()
        if patternId:
            return patternId
        else:
            raise self.TooManySimultaneousPatterns()

    def releasePatternId(self, patternId):
        self._idPool.append(patternId)

    def react(self, message):
        """

        :param message: Message
        :return: boolean
        """
        assert(isinstance(message, CommunicationMessage))
        received = False

        if isinstance(message, RequestReplyMessage):
            conversationId = message.getConversationId()
            if conversationId in self._onReplyReactionPatterns:
                patterns = list(self._onReplyReactionPatterns[conversationId].viewvalues())
                for pattern in patterns:
                    conversationId, patternId, callback = pattern
                    self._workerPool.enqueueTask(message, callback)
                    received = True
                    self.removeReactionPattern(pattern)

        if not received:
            for pattern in self._reactionPatterns.viewvalues():
                self._workerPool.enqueueTask(message, pattern)
                received = True

        return received


class BehaviouralPattern():

    class PatternNotStarted(Exception):
        pass

    class PatternAlreadyStarted(Exception):
        pass

    class PatternAlreadyTerminated(Exception):
        pass

    def __init__(self, agentIdentity):
        from agent.Agent import BaseAgentIdentity
        assert(isinstance(agentIdentity, BaseAgentIdentity))
        self._id = None
        self._agentIdentity = agentIdentity
        self._started = False
        self._terminated = False

    def setId(self, _id):
        self._id = _id

    def getId(self):
        return self._id

    def getAgentIdentity(self):
        """

        :rtype : AgentIdentity
        """
        from agent.Agent import BaseAgentIdentity
        assert isinstance(self._agentIdentity, BaseAgentIdentity)
        return self._agentIdentity

    def start(self):
        if self._started:
            raise self.PatternAlreadyStarted()
        self._start()
        self._started = True

    def _start(self):
        raise NotImplemented

    def stop(self):
        if not self._started:
            raise self.PatternNotStarted
        if self._terminated:
            raise self.PatternAlreadyTerminated
        self._stop()
        self._terminated = True

    def _stop(self):
        raise NotImplemented


class ActionPattern(BehaviouralPattern):

    class Runner(threading.Thread):

        def __init__(self, actionPattern):
            threading.Thread.__init__(self)
            self._actionPattern = actionPattern

        def run(self):
            self._actionPattern.process()

    def __init__(self, agentIdentity):
        """

        :param agentIdentity: AgentIdentity
        """
        BehaviouralPattern.__init__(self, agentIdentity)

    def process(self):
        raise NotImplemented

    def _start(self):
        self.Runner(self).start()

    def _stop(self):
        pass


class ReactionPattern(BehaviouralPattern):

    def __init__(self, agentIdentity, parentReactionPattern=None, oneShot=False):
        BehaviouralPattern.__init__(self, agentIdentity)
        self._parentReactionPattern = parentReactionPattern
        self._oneShot = oneShot

    def acceptMessage(self, message):
        """

        :param message: Message
        :raise NotImplemented:
        :rtype boolean
        """
        raise NotImplemented

    def receiveMessage(self, message):
        """


        :raise NotImplemented:
        """
        raise NotImplemented

    def enter(self):
        pass

    def exit(self):
        pass

    def _start(self):
        self.enter()

    def _stop(self):
        self.exit()

