from pandas import DataFrame
from py2neo import Graph
from py2neo.bulk import merge_relationships, merge_nodes
import pandas as pd

class NeoFrame:
    """
    Neo4jFrame to give dataframe functionality to DataFrames

    """

    def __init__(
            self,
            dataframe: DataFrame,
            graph=None,
            nodes: list = None,
            edges: dict = None,
            node_attributes: dict = {},
            edge_attributes: dict = {},
            add_constraint_flag: bool = True
    ):

        self.frame = dataframe
        self.node_map = None
        self.g = graph
        self.node_map = {}
        self.node_keys = {}
        self.node_data = {}
        self.edge_map = {}
        self.edge_keys = {}
        self.edge_data = {}

        if nodes:
            self.add_nodes(nodes=nodes, node_attributes=node_attributes)
            if add_constraint_flag:
                self.add_constraints(nodes=nodes)

        if edges:
            self.add_edges(edges=edges, edge_attributes=edge_attributes)

    @staticmethod
    def _get_node_attributes(df: DataFrame, attribute_cols: list) -> list:
        attr = []
        for a in attribute_cols:
            attr.append(df[a].values)

        return attr

    # TODO add new attributes after graph is created
    def add_attributes(self, attribute_dict: dict) -> None:
        """
        Add attributes from data

        :param attribute_dict: dicts from data arguments
        :type attribute_dict: dict
        """
        if self.node_map is None:
            return
        else:
            for node_label in attribute_dict:
                if node_label in self.node_map:
                    self.node_map[node_label]['key'].append(attribute_dict[node_label]['key'])

    def add_nodes(self, nodes: list, node_attributes: dict) -> None:
        """
        Adds nodes from the dataframe columns

        :param nodes:
        :type nodes: list
        :param node_attributes:
        :type node_attributes: dict
        """
        for node_label, node_col in nodes:
            if node_label in node_attributes:
                attr_cols = node_attributes[node_label]
            else:
                attr_cols = []
            self.node_map[node_label] = self.frame[node_col].values
            self.node_keys[node_label] = [node_col] + attr_cols
            df = self.frame[~self.frame[node_col].isna()]
            self.node_data[node_label] = list(zip(df[node_col].values,
                                                  *self._get_node_attributes(df=df, attribute_cols=attr_cols)))

    def add_constraints(self, nodes: list):
        """
        Adds constraint to the neo4j graph. This speeds up creating the grpah when merging nodes

        :param nodes:
        :type nodes: list
        """
        for node_label, node_col in nodes:
            q = """
            CREATE CONSTRAINT IF NOT EXISTS FOR (n:{}) REQUIRE n.{} IS UNIQUE
            """.format(node_label, node_col)

            self.g.run(q)

    def _get_edge_attributes(self, attribute_cols: list) -> list:
        attr_data = []
        for index, row in self.frame.iterrows():
            attr_dict = {}
            for col in attribute_cols:
                attr_dict[col] = row[col]
            attr_data.append(attr_dict)
        return attr_data

    def add_edges(self, edges: dict, edge_attributes: dict) -> None:
        """
        Adds edges from the dataframe columns

        :param edges:
        :type edges: dict
        :param edge_attributes: attribute mappings for the user defined g
        :type edge_attributes: dict
        """
        for edge in edges:
            source_label, target_label = edge
            source_col, target_col = self.node_keys[source_label][0], self.node_keys[target_label][0]
            source_data = self.node_map[source_label]
            target_data = self.node_map[target_label]
            if edge in edge_attributes:
                attr_col = edge_attributes[edge]
            else:
                attr_col = []
            attr_data = self._get_edge_attributes(attr_col)
            self.edge_map[edge] = edges[edge]  # relationship
            self.edge_keys[edge] = ((source_label, source_col), (target_label, target_col))  # start and end key
            edge_data = []
            for e in list(zip(source_data, attr_data, target_data)): # edge data in neo4j format
                if any([pd.isnull(x) for x in e]):
                    continue
                edge_data.append(e)
            self.edge_data[edge] = edge_data

    def create_graph(self) -> None:
        """
        Create graph using the above utility functions to enter
        user formatted data into a Neo4j graph

        """

        if self.g is None:
            self.g = Graph()

        if self.node_map:
            for node_label in self.node_map:
                data = self.node_data[node_label]
                keys = self.node_keys[node_label]
                merge_key = (node_label, keys[0])  # first element in keys is the node column
                merge_nodes(self.g.auto(), data, merge_key, keys=keys)

        if self.edge_map:
            for edge in self.edge_map:
                data = self.edge_data[edge]
                rel_type = self.edge_map[edge]
                start_node_key, end_node_key = self.edge_keys[edge]
                merge_relationships(self.g.auto(), data, merge_key=rel_type, start_node_key=start_node_key,
                                    end_node_key=end_node_key)