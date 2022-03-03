import os

from pymongo import MongoClient
from os import getenv
import logging
from enum import Enum
from typing import Optional, List
from time import sleep
from colorama import Fore
"""
Simple proof of concept showing how to gather LogicalSession information from a single shard sharded cluster on Atlas.

This POC could be expanded to cover multiple shard clusters as well.

We first connect to the cluster using the standard SRV record, and then re-connect to port 27017 to discover
Topology from the mongod colocated with the mongos we connect to.

We then pull Active Session information from each member discovered.

username, password, and host are set as ENV variables to connect to the cluster.


"""
username = getenv("mongo_username")
password = getenv("mongo_password")
host = getenv("mongo_host", "adltest.l50ex.mongodb.net")
"The host found in the Atlas SRV connection string."
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
max_runs: int = 10000
"maximum number of times to grab session info"
interval_secs: int = 5
"Pause time between grabbing session info"
rs_port = 27017
"The standard port where mongod instances run on Atlas"


client = MongoClient(
    f"mongodb+srv://{username}:{password}@{host}/transactions?retryWrites=true&w=majority")
"Initial client used to connect to the cluster using the standard SRV record method."

class States(Enum):
    STARTUP = 0
    PRIMARY = 1
    SECONDARY = 2
    RECOVERING = 3
    STARTUP2 = 5
    UNKNOWN = 6
    ARBITER = 7
    DOWN = 8
    ROLLBACK = 9
    REMOVED = 10


class LogicalSessions:
    def __init__(self, source_dict: dict) -> None:
        """Information regarding the local logical sessions cache.

        This is only a subset of the information returned from server status.

        :param source_dict: The dict output of server status 'logicalSessionRecordCache' key.
        """

        self.active_session_count: int = source_dict.get("activeSessionsCount")
        "The number of all active local sessions cached in memory by the mongod or mongos instance " \
            "since the last refresh period."
        self.session_catalog_size: int = source_dict.get("sessionCatalogSize")
        "For a mongod instance: " \
            "The size of its in-memory cache of the config.transactions entries. " \
            "This corresponds to retryable writes or" \
            " transactions whose sessions have not expired within the localLogicalSessionTimeoutMinutes." \
            "For a mongos instance:" \
            "The number of the in-memory cache of its sessions that have had transactions within the most recent " \
            "localLogicalSessionTimeoutMinutes interval."
        self.last_transactionReaper_job_duration_millis: int = source_dict.get("lastTransactionReaperJobDurationMillis")
        "The length (in milliseconds) of the last transaction record cleanup. This can indicate how 'hard; the " \
            "instance is working to keep the sessions cache in synch"


class Member:
    def __init__(self, source_dict: dict):
        """Class for Replica set member information.

        Includes method to
        :param source_dict: Dict out put of the replica set status 'member' key
        """
        self._id: int = source_dict.get('_id')
        self.name: str = source_dict.get('name').split(":")[0]
        self.port: int = int(source_dict.get('name').split(":")[1])
        self.state: States = States(source_dict.get('state'))
        self.server_status: Optional[dict] = None
        self.session_info: Optional[LogicalSessions] = None

    def get_server_status(self, username: str, password: str) -> dict:
        """Fetches server.status for the Member

        :param username: data access user
        :param password: data access password
        :return: dict: a dictionary of full server status.
        """
        try:
            local_client = MongoClient(host=self.name, port=self.port, username=username, password=password, ssl=True)
            server_status = local_client.admin.command({"serverStatus": 1})
            self.server_status = server_status
            logger.info(f"Sucesfully connected to {self.name} which is a {self.state.name}")
            return server_status
        except Exception as e:
            logger.error(f"Could not retrieve server status from {self.name}")
            raise IOError(e)

    def get_session_info(self) -> LogicalSessions:
        """Sets and returns logical session information for the Member

        :return: A LogicalSessions object with current logical sessions data.
        """
        self.get_server_status(username, password)
        self.session_info: LogicalSessions = LogicalSessions(self.server_status["logicalSessionRecordCache"])
        return self.session_info


class Topology:
    def __init__(self, mongos_client: MongoClient) -> None:
        """Holds the cluster topology as discovered from the mongos.


        :param mongos_client: A configured mongodb client, which connects to the cluster using SRV.
        """
        self.host_list: list = list()
        self.uri_base: str = mongos_client._MongoClient__init_kwargs["host"].split("@")[1].split("/")[0]
        for each_node in mongos_client.topology_description.server_descriptions():
            self.host_list.append(each_node[0])
        self.host_count: int = len(self.host_list)

        self.seed_host: MongoClient = MongoClient(host=self.host_list[0], port=27017, username=username,
                                                  password=password, ssl=True)
        self.repl_status: dict = self.seed_host.admin.command({'replSetGetStatus': 1})
        self.repl_set_name: str = self.repl_status["set"]
        self.seed_host_state: States = States(self.repl_status["myState"])
        self.members: List[Member] = list()
        # Creates a Member object for each RS member and appends to the members attribute
        for each_member in self.repl_status.get('members', []):
            self.members.append(Member(each_member))


topology = Topology(client)

i = 1

while i < max_runs:
    for each_member in topology.members:
        session_info = each_member.get_session_info()
        print(
            Fore.GREEN + f"[{i}]" + Fore.BLUE + f" Member {each_member.name} " + Fore.WHITE +
            f"({each_member.state.name}) ".ljust(12) + Fore.CYAN
            + "- Active Sessions = " + Fore.CYAN + f"{session_info.active_session_count}")
    i += 1
    sleep(interval_secs)
