__author__ = 'martin'

from agent.Agent import *
#from MTL import getMTLImplementation

if __name__ == "__main__":
    platform_manager = FluxyManager()
    ##platform_manager.bindToMTLActor(influx_spade.SpadeAgentActor("manager@ophelia.localdomain", "secret"))
    platform_manager.bindToMTLAgent(getMTLImplementation().getPlatformManagerMTLAgent())
    platform_manager.startPlatform()