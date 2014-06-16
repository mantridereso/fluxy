__author__ = 'martin'


from agent.Agent import *
import math

# find prime numbers in range 1..MAX_N
MAX_N = 10000


class TestNumberRequestMessage(RequestMessage):

    MESSAGE_TYPE = "prime_test_number"

    KEY_NUMBER = "number"

    def __init__(self, number=None):
        RequestMessage.__init__(self)
        if not number is None:
            self._setPayloadEntry(self.KEY_NUMBER, number)

    def _payloadIsWellFormed(self):
        return self._hasPayloadEntry(self.KEY_NUMBER)

    def getNumber(self):
        return self._getPayloadEntry(self.KEY_NUMBER)

    def parsePayload(self, payload):
        if self.KEY_NUMBER in payload:
            self._setPayloadEntry(self.KEY_NUMBER, payload[self.KEY_NUMBER])


class NotAPrimeNumberMessage(RequestReplyMessage):

    MESSAGE_TYPE = "not_a_prime_number"

    def _payloadIsWellFormed(self):
        return True


class CouldBeAPrimeNumberMessage(RequestReplyMessage):

    MESSAGE_TYPE = "could_be_a_prime_number"

    def _payloadIsWellFormed(self):
        return True


class SubscribeForNumbersGreaterNMessage(InformMessage):

    MESSAGE_TYPE = "subscribe_for_greater_n"
    KEY_N = "n"

    def __init__(self, n=None):
        InformMessage.__init__(self)
        if not n is None:
            self._setPayloadEntry(self.KEY_N, n)

    def getN(self):
        return self._getPayloadEntry(self.KEY_N)

    def _payloadIsWellFormed(self):
        return self._hasPayloadEntry(self.KEY_N)

    def parsePayload(self, payload):
        if self.KEY_N in payload:
            self._setPayloadEntry(self.KEY_N, payload[self.KEY_N])


class NNeedsPrimeTestingMessage(InformMessage):

    MESSAGE_TYPE = "n_needs_testing"

    KEY_N = "n"

    def __init__(self, n=None):
        InformMessage.__init__(self)
        if not n is None:
            self._setPayloadEntry(self.KEY_N, n)

    def getN(self):
        return self._getPayloadEntry(self.KEY_N)

    def _payloadIsWellFormed(self):
        return self._hasPayloadEntry(self.KEY_N)

    def parsePayload(self, payload):
        if self.KEY_N in payload:
            self._setPayloadEntry(self.KEY_N, payload[self.KEY_N])


class ReadyMessage(InformMessage):
    MESSAGE_TYPE = "ready"

    def _payloadIsWellFormed(self):
        return True


class DoneMessage(InformMessage):
    MESSAGE_TYPE = "calc_done"

    def _payloadIsWellFormed(self):
        return True


class BlackBoard(Agent):

    # the BlackBoard Agent is in both groups: master group and experts group.
    # The master agent in master group writes problems on the blackboard
    # that need to be solved (problems are: "is n=<x> a prime number or not?")
    # The expert agents in experts group subscribe against the blackboard for
    # numbersToTest > NUM and will then receive notifications of problems n>NUM.
    # The BlackBoard agent coordinates the expert agents (i.e. requests evaluation
    # from the experts)
    # The expert agents will "write" partial solutions for single problems on the
    # blackboard like "<x> is not a prime number" or "<x> could be a prime number".
    # The BlackBoard agent integrates the partial solutions to complete solutions and
    # notifies the master actor in master group.

    GROUP_MASTER = "master_group"
    GROUP_EXPERTS = "expert_group"

    ROLE_BLACKBOARD = "blackboard"
    ROLE_MASTER = "master"
    ROLE_EXPERT = "expert"

    class SubscribeReactionPattern(ReactionPattern):

        # handle expert subscription for problems with nToTest>n

        def __init__(self, actorIdentity):
            ReactionPattern.__init__(self, actorIdentity)

        def acceptMessage(self, message):
            return isinstance(message, SubscribeForNumbersGreaterNMessage)

        def receiveMessage(self, message):
            assert isinstance(message, SubscribeForNumbersGreaterNMessage)
            board = self.getAgentIdentity().getAgent()
            assert isinstance(board, BlackBoard)
            board.addSubscription(message.getN(), message.getSender())

    class RegisterProblemReactionPattern(ReactionPattern):

        # handle publishing of problems to the blackboard (publishing is done by master agent)

        def acceptMessage(self, message):
            return isinstance(message, NNeedsPrimeTestingMessage)

        def receiveMessage(self, message):
            assert isinstance(message, NNeedsPrimeTestingMessage)
            print("Problem written onto Blackboard: Is n="+str(message.getN())+" a prime number?")
            board = self.getAgentIdentity().getAgent()
            assert isinstance(board, BlackBoard)
            board.onNewProblem(message.getN())

    def __init__(self):
        Agent.__init__(self)
        self._subscriptions = dict()
        self._subscriptionsPending = 0
        self._openProblems = dict()

    def addSubscription(self, n, actorIdentity):
        if not n in self._subscriptions:
            self._subscriptions[n] = []
        self._subscriptions[n].append(actorIdentity)
        self._subscriptionsPending -= 1
        if self._subscriptionsPending == 0:
            # all experts have subscribed...

            def readyInformMaster(actors):
                # notify master that he can start to write problems on blackboard

                masterBoardSelf = self \
                    .inRole(self.ROLE_BLACKBOARD) \
                    .withinGroup(self.GROUP_MASTER)

                masterBoardSelf \
                    .getBehaviour() \
                    .addReactionPattern(self.RegisterProblemReactionPattern(masterBoardSelf))

                if len(actors) > 0:
                    master = actors[0]
                    masterBoardSelf \
                        .sendInformMessage(ReadyMessage()) \
                        .to(master)

            self \
                .inRole(self.ROLE_BLACKBOARD) \
                .withinGroup(self.GROUP_MASTER) \
                .withAgentsInRole(PrimeNumberTestMaster.ROLE_MASTER) \
                .do(readyInformMaster)

    def onNewProblem(self, nToTest):
        self._openProblems[nToTest] = True
        boardSelf = self \
            .inRole(BlackBoard.ROLE_BLACKBOARD) \
            .withinGroup(BlackBoard.GROUP_EXPERTS)
        expertsToAsk = []
        for n, actorList in self._subscriptions.items():
            if nToTest > n:
                for actor in actorList:
                    expertsToAsk.append(actor)

        def continueAskExperts():
            if len(expertsToAsk)>0:
                # ask next expert
                nextExpert = expertsToAsk.pop()
                boardSelf \
                    .sendRequest(TestNumberRequestMessage(nToTest)) \
                    .to(nextExpert) \
                    .reactUponReply(processReply)
            else:
                # all experts have been asked. Seems to be a prime number.
                if nToTest in self._openProblems:
                    del self._openProblems[nToTest]
                    print(str(nToTest) + " IS a prime number")
                    self.testIfDone()

        def processReply(message):
            if isinstance(message, NotAPrimeNumberMessage):
                # if one expert says the number is not a prime number it isn't
                # a prime number. No need to ask the remaining experts.
                if nToTest in self._openProblems:
                    del self._openProblems[nToTest]
                    print(str(nToTest) + " is not a prime number")
                    self.testIfDone()
            elif isinstance(message, CouldBeAPrimeNumberMessage):
                continueAskExperts()

        continueAskExperts()

    def testIfDone(self):
        if len(self._openProblems)==0:
            # all problems on blackboard solved. Notify master.
            masterBoardSelf = self \
                .inRole(BlackBoard.ROLE_BLACKBOARD) \
                .withinGroup(BlackBoard.GROUP_MASTER)

            def notifyMasterOnDone(masters):
                if len(masters)>0:
                    master = masters[0]
                    masterBoardSelf.sendInformMessage(DoneMessage()).to(master)

            masterBoardSelf.withAgentsInRole(BlackBoard.ROLE_MASTER).do(notifyMasterOnDone)

    def onEnterPlatform(self):
        # blackboard enters fluxy platform
        def _nothing(message):
            pass

        def expertGroupEntered(message):
            # blackboard obtained role in experts group
            print("blackboard obtained role in experts group")
            blackBoardSelf = self \
                .inRole(self.ROLE_BLACKBOARD) \
                .withinGroup(self.GROUP_EXPERTS)
            blackBoardSelf \
                .getBehaviour() \
                .addReactionPattern(self.SubscribeReactionPattern(blackBoardSelf))

            class FactorExpertLauncher():

                def __init__(self, factorInteger):
                    self._factor = factorInteger

                def run(self):
                    factorExpert = FactorExpert(self._factor)
                    factorExpert.bindToMTLAgent(getMTLImplementation().getMTLAgent("factor_expert_"+str(self._factor)))

                    factorExpert.enterPlatform()

            #create experts for factors 2..squareroot(MAX_N)
            for n in range(2, int(math.ceil(math.sqrt(MAX_N)))):
                if (n == 2) or (n == 3) or (n % 2 > 0) and (n % 3 > 0):
                    expertLauncher = FactorExpertLauncher(n)
                    expertLauncher.run()
                    self._subscriptionsPending += 1

        def expertsGroupCreated(message):
            #request blackboard role in experts group
            print("request blackboard role in experts group")
            self \
                .inBaseRole() \
                .requestRole(self.ROLE_BLACKBOARD) \
                .withinGroup(self.GROUP_EXPERTS) \
                .onSuccess(expertGroupEntered) \
                .onGroupNotFound(_nothing).onDenied(_nothing)

        def masterGroupEntered(message):
            #create experts group
            print("create experts group")
            self \
                .inBaseRole() \
                .createGroup(Group(self.GROUP_EXPERTS)) \
                .onSuccess(expertsGroupCreated) \
                .onFail(_nothing)

        #join master group
        self \
            .inBaseRole() \
            .requestRole(self.ROLE_BLACKBOARD) \
            .withinGroup(self.GROUP_MASTER) \
            .onSuccess(masterGroupEntered) \
            .onDenied(_nothing) \
            .onGroupNotFound(_nothing)


class FactorExpert(Agent):

    # a FactorExpert(factor) computes (nToTest modulo factor). If the result equals 0
    # a number to test (nToTest) can not be a prime number. Otherwise the number to test
    # can (could) be a prime number.

    class NumberTesting(ReactionPattern):

        # main expert behavioural pattern. Reply to "is x a prime number?" - requests.
        # Either answer "not a prime number" or answer "could be a prime number".

        def __init__(self, actorIdentity, parentReactionPattern=None, oneShot=False):
            ReactionPattern.__init__(self, actorIdentity,
                                            parentReactionPattern, oneShot)
            self._factor = actorIdentity.getAgent().getFactor()

        def acceptMessage(self, message):
            return isinstance(message, TestNumberRequestMessage)

        def receiveMessage(self, message):
            assert isinstance(message, TestNumberRequestMessage)
            if message.getNumber() % self._factor == 0:
                reply = NotAPrimeNumberMessage()
            else:
                reply = CouldBeAPrimeNumberMessage()
            self.getAgentIdentity() \
                .inReplyTo(message) \
                .sendMessage(reply)

    def __init__(self, factorInteger):
        Agent.__init__(self)
        self._factor = factorInteger

    def getFactor(self):
        return self._factor

    def onEnterPlatform(self):

        # expert enters fluxy platform

        def _nothing(message):
            pass

        def subscribe(actors):
            if len(actors)>0:

                expertSelf = self \
                    .inRole(BlackBoard.ROLE_EXPERT) \
                    .withinGroup(BlackBoard.GROUP_EXPERTS)

                expertSelf.getBehaviour().addReactionPattern(self.NumberTesting(expertSelf))

                board = actors[0]
                expertSelf \
                    .sendInformMessage(SubscribeForNumbersGreaterNMessage((self.getFactor()**2)-1)) \
                    .to(board)

        def expertRoleObtained(m):
            print("factor expert for n="+str(self._factor)+" joined expert group")
            # Agent is expert in expert group now.
            # subscribe at blackboard for problems with nToTest > factor*factor
            # (traditional optimization for prime number testing: test only for factors smaller or equal
            # to the square root of nToTest)
            self \
                .inRole(BlackBoard.ROLE_EXPERT) \
                .withinGroup(BlackBoard.GROUP_EXPERTS) \
                .withAgentsInRole(BlackBoard.ROLE_BLACKBOARD) \
                .do(subscribe)

        # attempt to join expert group in expert role
        self \
            .inBaseRole() \
            .requestRole(BlackBoard.ROLE_EXPERT) \
            .withinGroup(BlackBoard.GROUP_EXPERTS) \
            .onSuccess(expertRoleObtained).onGroupNotFound(_nothing).onDenied(_nothing)


class PrimeNumberTestMaster(Agent):

    # the test master agent writes problems on the blackboard. Also it instantiates the
    # blackboard. The blackboard will notify the master agent when all problems on the
    # blackboard have been solved.

    ROLE_MASTER = "master"
    ROLE_FACTOR_EXPERT = "factor_expert"
    GROUP = "prime_group"

    class ReadyReaction(ReactionPattern):

        # ready reaction pattern fires when blackboard sends "ready" message to master
        # (ready=all experts in expert group are prepared to collaborate in solution process).
        # On Ready event the master starts to write problems on the blackboard

        def acceptMessage(self, message):
            return isinstance(message, ReadyMessage)

        def receiveMessage(self, message):
            masterSelf = self.getAgentIdentity().getAgent() \
                .inRole(BlackBoard.ROLE_MASTER) \
                .withinGroup(BlackBoard.GROUP_MASTER)

            def writeProblemsToBlackBoard(blackboards):
                if len(blackboards)>0:
                    board = blackboards[0]
                    for i in range(1,MAX_N):
                        masterSelf.sendInformMessage(NNeedsPrimeTestingMessage(i)).to(board)
                # start writing problems to blackboard
            masterSelf.withAgentsInRole(BlackBoard.ROLE_BLACKBOARD).do(writeProblemsToBlackBoard)

    class DoneReactionPattern(ReactionPattern):
        # blackboard sends message that all problems on the blackboard have been solved

        def acceptMessage(self, message):
            return isinstance(message, DoneMessage)

        def receiveMessage(self, message):

            from zmq_mtl.FluxyZMQ import ZMQAgent

            tSumRcv = ZMQAgent.Receiver.tSum
            rcvCntMessages = ZMQAgent.Receiver.cntMessages

            print("cntMessagesRcv = "+str(rcvCntMessages))
            print("tSumRcv = "+str(tSumRcv))

            print("DONE :-)")


    def __init__(self):
        Agent.__init__(self)

    def onEnterPlatform(self):

        #master agent enters fluxy platform

        def _nothing(message):
            print str(message.export())

        def masterGroupEntered(message):
            masterSelf = self.inRole(BlackBoard.ROLE_MASTER).withinGroup(BlackBoard.GROUP_MASTER)
            masterSelf.getBehaviour().addReactionPattern(self.ReadyReaction(masterSelf))
            masterSelf.getBehaviour().addReactionPattern(self.DoneReactionPattern(masterSelf))

            #create blackboard
            board = BlackBoard()
            board.bindToMTLAgent(getMTLImplementation().getMTLAgent("prime"+"board"))
            board.enterPlatform()


        def masterGroupCreated(message):
            # request role master in master group
            self \
                .inBaseRole() \
                .requestRole(BlackBoard.ROLE_MASTER) \
                .withinGroup(BlackBoard.GROUP_MASTER) \
                .onDenied(_nothing) \
                .onGroupNotFound(_nothing) \
                .onSuccess(masterGroupEntered)

        print("create master group")
        #create master group
        self \
            .inBaseRole() \
            .createGroup(Group(BlackBoard.GROUP_MASTER)) \
            .onSuccess(masterGroupCreated) \
            .onFail(_nothing)

####

# register prime_number example - specific Message classes in fluxy message parser
messageParser = MessageParser.getInstance()
messageParser.registerType(TestNumberRequestMessage)
messageParser.registerType(NotAPrimeNumberMessage)
messageParser.registerType(CouldBeAPrimeNumberMessage)
messageParser.registerType(ReadyMessage)
messageParser.registerType(SubscribeForNumbersGreaterNMessage)
messageParser.registerType(NNeedsPrimeTestingMessage)
messageParser.registerType(DoneMessage)


def runPrimeTesting():

    # runner (main entry point)

    #import yappi
    #yappi.stop()
    testMaster = PrimeNumberTestMaster()
    testMaster.bindToMTLAgent(getMTLImplementation().getMTLAgent("test_master" + "boss"))

    testMaster.enterPlatform()

    time.sleep(50)


####

if __name__ == "__main__":

    runPrimeTesting()


