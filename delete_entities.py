"""
Script to delete all entities on local IDF instance

Copyrights (c) Nutanix Inc. 2019

Author: akheel.km@nutanix.com
"""
#pylint:disable=import-error, no-name-in-module, broad-except, unused-import
import random
import string
import sys
import uuid

import env
import gflags

from google.protobuf.text_format import Merge
from insights_interface.insights_interface_pb2 import \
  BatchDeleteEntitiesArg, DeleteEntityArg, GetEntitiesWithMetricsArg, EntityGuid
from insights_interface.insights_interface import InsightsInterface

gflags.DEFINE_string("entity_type", "vm", "Entity type you want to create")

QUERY_STR = """
  query {
    entity_list {
      entity_type_name: ""
    }
    where_clause {
      lhs {
        comparison_expr {
          lhs {
            leaf {
              column: "_master_cluster_uuid_"
            }
          }
          operator: kExists
        }
      }
      operator: kNot
    }
    group_by {
      raw_limit {
        limit: 1000
      }
      raw_columns {
        column: "_cas_value_"
      }
    }
  }
"""

class DeleteEntities(object):
  """
  Class to delete all non cas entities
  """

  def __init__(self):
    """
    Initialising the variables
    """
    self.insights = InsightsInterface("127.0.0.1", "2027")
    self.entity_data_list = None

  @staticmethod
  def delete_entity_arg(entity_id, cas_value):
    """
    For a given entity id, create a DeleteEntityArg
    Args:
      entity_id(str): Entity id
      cas_value(int): Current cas value of the entity
    Returns:
       DeleteEntityArg: Arg for delete RPC
    """
    guid = EntityGuid()
    guid.entity_type_name = FLAGS.entity_type
    guid.entity_id = entity_id
    dea = DeleteEntityArg()
    dea.entity_guid.CopyFrom(guid)
    if cas_value is not False:
      dea.cas_value = cas_value + 1
    return dea

  def get_entity_data(self):
    """
    Get list of 1000 entities of the given type
    Returns:
       None
    """
    self.entity_data_list = []
    arg = GetEntitiesWithMetricsArg()
    Merge(QUERY_STR, arg)
    arg.query.entity_list[0].entity_type_name = FLAGS.entity_type
    ret = self.insights.GetEntitiesWithMetrics(arg)
    if ret.total_group_count == 0:
      return
    for each in ret.group_results_list[0].raw_results:
      e_id = each.entity_guid.entity_id
      if len(each.metric_data_list[0].value_list) == 1:
        cas_value = each.metric_data_list[0].value_list[0].value.uint64_value
      else:
        cas_value = False
      self.entity_data_list.append((e_id, cas_value))

  def batch_delete(self):
    """
    Batch delete list of entities
    """
    arg = BatchDeleteEntitiesArg()
    delete_args = []
    for e_id, cas_value in self.entity_data_list:
      delete_args.append(self.delete_entity_arg(e_id, cas_value))
    arg.entity_list.extend(delete_args)
    print "Deleting %s entities" % len(delete_args)
    try:
      self.insights.BatchDeleteEntities(arg)
    except Exception as error:
      print error.message

  def run(self):
    """
    Method to delete all cas entities of given type
    """
    while True:
      self.get_entity_data()
      if not self.entity_data_list:
        break
      self.batch_delete()
    print "Deleted all entities"


if __name__ == "__main__":
  FLAGS = gflags.FLAGS
  FLAGS(sys.argv)
  DeleteEntities().run()
