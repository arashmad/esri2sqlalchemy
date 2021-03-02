# -*- coding: utf-8 -*-

try:
  import os
  import os.path as path
  import json
  import xmltodict
  import codecs
  import glob
  import re
  from datetime import datetime
  
except Exception as e:
    print '\n====================================='
    print 'Failed to run script!'
    print 'Some of the requirements are missing.'
    print 'More Information =>'
    print e
    raise Exception("Terminated")


interface_header = """
# -*- coding: utf-8 -*-
import json
import inspect
import sys

from datetime import datetime
from sqlalchemy.orm import relationship, composite, sessionmaker, scoped_session
from sqlalchemy import Table, Column, UniqueConstraint, ForeignKey, PrimaryKeyConstraint, ForeignKeyConstraint
from sqlalchemy import DateTime, Boolean, Text, NVARCHAR, VARCHAR, String, Integer, Float, Unicode, SmallInteger, BigInteger
from sqlalchemy import create_engine, MetaData
from sqlalchemy import or_, func, desc, asc
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy_utils import ChoiceType, EmailType, PasswordType, PhoneNumber, ColorType
from geoalchemy2 import Geometry
from passlib.handlers import pbkdf2, md5_crypt
from passlib import hash
from state_machine import acts_as_state_machine, State, Event, before, after

Base = declarative_base()
Base.metadata.schema = 'api'


"""

excluded_tables =[
  '_layer_role', '_user_role', '_role_permission', 'regions', 'roles', 'departments'
  # 'flood', 'dam_o', 'rain', 'wellPiezometer_o', 'sediment','gps_o', 'wind', 'thunder', 'dam_device'
  # 'plansss', 'projects', 'dam_lab', 'waterQuality', 'contract', 'evaporation', 'waterLevel', 'network_o',
  # 'waterFlow', 'drinkQuality', 'wells', 'persons', 'users', 'layers', 'geostyler', 'company'
  ]

def file_exist(__path__):
  if path.exists(__path__):
    return True
  else:
    return False

def parse_xml(__path__):
  if file_exist(__path__):
    f = open(__path__, "r")
    try:
      xml_dict = xmltodict.parse(f)
    except:
      xml_dict = {}
    return xml_dict
  else:
    raise Exception("File doesn't exist!")

def export_file(content, __path__):
  try:
    file = codecs.open(__path__, "w", "utf-8")
    file.write(content)
    file.close()
    # print('\n######################')
    # print('-----------------------')
    # print('File %s is ready now!' % __path__)
    # print('-----------------------')
    # print('######################\n')
  except:
    print('\n++++++++++++++++++++++++++++++')
    print('-------------------------------')
    print('Error in creating %s!' % __path__)
    print('-------------------------------')
    print('++++++++++++++++++++++++++++++\n')

def find_tb_group(content):
  group_en = ''
  for item in content.split('/'):
    if 'FD=hormozgan.DBO.' in item:
      group_en = item.split('.')[-1]
  if group_en == '':
    group_en = 'unknown'
  return group_en

def translate(name):
  if name == 'baseLayers':
    return 'اطلاعات کشوری'
  elif name == 'basics':
    return 'پایه'
  elif name == 'facilities':
    return 'سد و تاسیسات آبی'
  elif name == 'hydrology':
    return 'هیدرولوژی'
  elif name == 'hydrogeology':
    return 'هیدروژئولوژی'
  elif name == 'informatics':
    return 'فناوری اصلاعات'
  elif name == 'projects':
    return 'طرح و پروژه ها'
  elif name == 'rivers':
    return 'مهندسی رودخانه'
  elif name == 'quality':
    return 'محیط زیست و کیفیت منابع آب'
  elif name == 'unknown':
    return 'سایر'

def normal_field(name):
  temp = ''
  output = ''
  chars = list(name)
  for i in range(len(chars)):
      if chars[i] == chars[i].lower():
          if temp != '':
              output += temp
              # if len(temp) > 1 and chars[i] != '_':
              #     output += '_'
              temp = ''
          output += chars[i]
      else:
          temp += chars[i].lower()
  output += temp

  if output in ['id','class','type']:
      output += '_'
  return output

def now_date_time():
    try:
        now = datetime.now()
        return now.strftime("%Y.%m.%d %H.%M.%S")
    except Exception as e:
        print e
        raise('ERROR!')
       
def create_dictionary(table_array, __path__):
  result_name = 'dictionary.js'
  result = ''
  for table in table_array:
    
    # tb_type = table['DatasetType'].replace('esriDT', '')
    tb_name = table['Name'].split('.')[-1]
    if tb_name == 'Valve':
      a = 1
      
    tb_columns = table['Fields']['FieldArray']['Field']

    dictionary = ''
    dictionary_domain = ''
    for column in tb_columns:
      col_name = column['Name']
      if col_name == 'OBJECTID': continue

      if 'AliasName' in column: col_alias_name = column['AliasName']
      else: col_alias_name = col_name
      if '\n' in col_alias_name:
        col_alias_name = col_alias_name.replace('\n', ' ')

      if 'Domain' in column:
        domain = column['Domain']
        domain_type = domain['@xsi:type']
        domain_name = column['Name']

        if '/' in domain_name:
          domain_name = domain_name.replace('/', '_')
        
        tmp = ''
        if domain_type == 'esri:CodedValueDomain':
          domain_touples = domain['CodedValues']['CodedValue']
          try:
            domain_touple_code = domain_touples['Code']['#text']
            domain_touple_name = domain_touples['Name']
            tmp += "\t\t'%s': '%s'\n" % (domain_touple_code, domain_touple_name)

          except:
            for domain_touple in domain_touples:
              if domain_touple == '@xsi:type':
                continue
              domain_touple_code = domain_touple['Code']['#text']
              domain_touple_name = domain_touple['Name']
            
              tmp += "\t\t'%s': '%s',\n" % (domain_touple_code, domain_touple_name)
          
          new_domain_name = normal_field(domain_name)
          dictionary_domain += '\n\tvalues_%s: {\n%s\t},\n' % (new_domain_name, tmp)

        # elif domain_type == 'esri:RangeDomain':
        #   a = 2
      
      new_col_name = normal_field(col_name)
      dictionary += "\t'%s': '%s',\n" % (new_col_name, col_alias_name)
    
    if dictionary_domain:
      dictionary = dictionary + dictionary_domain
     
    result += '_dictionary.%s = {\n%s};\n' % (re.sub(r"(\w)([A-Z])", r"\1_\2", tb_name).lower().strip(), dictionary)

  file_path = __path__ + "\\" + result_name
  export_file(result, file_path)

def create_interfaces(all_tables, all_relations, __path__):
  result_name = 'db_interface.py'
  
  interface = ""
  temp = ''
  for table in all_tables:
    # tb_type = table['DatasetType'].replace('esriDT', '')
    tb_name = table['Name'].split('.')[-1]

    print tb_name
    if tb_name == u'RefinaryPnt':
      a = 1

    if tb_name in excluded_tables: continue


    relationships = []
    relation_definition = table['RelationshipClassNames']
    if relation_definition:
      try:
        relation_tags = relation_definition['Name']
        if type(relation_tags) != list:
          tmp = []
          tmp.append(relation_tags)
          relation_tags = tmp

        for relaion_item in relation_tags:
          relation_object = {}
          for relation_table in all_relations:
            if relaion_item == relation_table['Name']:

              relation_type = relation_table['Cardinality'].replace('esriRelCardinality', '')
              # ================================== #
              # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! #
              # Create table for many-to-many type #
              # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! #
              # ================================== #
              relation_from = relation_table['OriginClassNames']['Name'].replace('hormozgan.DBO.', '')
              relation_to = relation_table['DestinationClassNames']['Name'].replace('hormozgan.DBO.', '')
              # relation_f_lbl = relation_table['ForwardPathLabel']
              # relation_b_lbl = relation_table['BackwardPathLabel']

              relation_object = {
                'type': relation_type,
                'from': relation_from,
                'to': relation_to
              }

              all_keys = relation_table['OriginClassKeys']['RelationshipClassKey']
              for key in all_keys:
                key_name = key['ObjectKeyName']
                key_role = key['KeyRole'].replace('esriRelKeyRoleOrigin', '')
                if key_role == 'Primary':
                  relation_object['PK'] = key_name
                elif key_role == 'Foreign':
                  relation_object['FK'] = key_name
              break
          relationships.append(relation_object)
              
      except:
        a = 1

    interface0 = ''
    interface_relations = ''
    
    interface0 = "class %s(Base):\n" % tb_name.title()
    interface0 += "\t__tablename__  = '%s'\n\n" % re.sub(r"(\w)([A-Z])", r"\1_\2", tb_name).lower().strip()
    
    interface0 += "\tid = Column(Integer, primary_key=True, autoincrement=True)\n"
    # interface0 += "\tName = Column(Unicode(100), nullable=True)\n"

    tb_columns = table['Fields']['FieldArray']['Field']

    # if tb_name.lower() == 'plainsheights':
    #     a = 1
        
    for column in tb_columns:
      col_type = column['Type'].replace('esriFieldType','')
      col_name = column['Name']
      
      # if tb_name == 'dam_o':
      #   print col_name

      #===================================================================================
      ################################################################## Change Column Name!!
      if col_name == 'class':
        col_name = '_class'
      if col_name == 'id':
        col_name = '_id'
        continue
      
      #===================================================================================
      ################################################################## Exclude Columns!!
      if 'hormozgan.dbo' in col_name.lower() or 'shape.' in col_name.lower():
        continue

      excluded_columns = [
        'views', 'created_user', 'created_date', 'last_edited_user', 'last_edited_date',
        'x', 'y', 'z', 'length', 'length_3d', 'perimeter', 'area', 'ATTACHMENTID']

      if col_name in excluded_columns:
        continue

      ################################################################## Exclude Columns!!
      #===================================================================================

      if col_name != 'OBJECTID':
        if col_type == 'Geometry':


          geometry_definition = column['GeometryDef']
          g_type = geometry_definition['GeometryType'].replace('esriGeometry','').upper()

          if g_type == 'POLYLINE':
            g_type = 'MULTILINESTRING'
            
          if g_type == 'POLYGON':
            g_type = 'MULTIPOLYGON'
          
          g_dim = 2
          if geometry_definition['HasZ'] == 'true':
            g_dim += 1
          if geometry_definition['HasM'] == 'true':
            g_dim += 1
          
          g_srid = '4326'

          interface0 += "\n\n\tgeom = Column(Geometry('%s', dimension=%s, srid=%s))\n" % (g_type, g_dim, g_srid)

          if (g_type == 'POINT'):
            interface0 += "\tx = Column(Float(precision=38), nullable=False)\n"
            interface0 += "\ty = Column(Float(precision=38), nullable=False)\n"
            # interface0 += "\tzone = Column(Integer)\n"
            if geometry_definition['HasZ'] == 'true':
              interface0 += "\tz = Column(Float(precision=38), nullable=False)\n"

          elif (g_type == 'POLYLINE'):
            interface0 += "\tlength = Column(Float(precision=2), nullable=True)\n"
            if geometry_definition['HasZ'] == 'true':
              interface0 += "\tlength_3d = Column(Float(precision=2), nullable=True)\n"

          elif (g_type == 'POLYGON'):
            interface0 += "\tlength = Column(Float(precision=2), nullable=True)\n"
            interface0 += "\tarea = Column(Float(precision=2), nullable=True)\n"

        else:
          if col_type == 'Single' or col_type == 'Double':
            f_scale = column['Scale']
            if f_scale == '0':
              f_type = 'Float(precision=2)'
            else:
              f_type = 'Float(precision=%s)' % f_scale
            
          elif col_type == 'Integer':
            f_type = 'Integer'

          elif col_type == 'SmallInteger':
            f_type = 'SmallInteger'

          elif col_type == 'Date':
            f_type = 'DateTime(timezone=False)'

          elif col_type == 'String':
            f_type = 'Text(convert_unicode=True)'
          
          f_nullable = ''
          if column['IsNullable'] == 'true':
            f_nullable = 'nullable=True'
          
          # f_unique = ''
          # f_required = ''
          # if (column['Required']) == 'true':
          #   f_required = ''

          # FK || PK => unique
          f_f_key = ''
          f_unique = ''
          if len(relationships):
            # print relationships
            for relationship in relationships:
              tb_parent = relationship['from']
              pk_ = relationship['PK']
              tb_child = relationship['to']
              fk_ = relationship['FK']

              if pk_ == 'id': pk_ = '_' + pk_
              if fk_ == 'id': fk_ = '_' + fk_

              if fk_ == col_name and tb_child == tb_name:
                # print(tb_name)
                foreign_relation = "'%s.%s'" % (re.sub(r"(\w)([A-Z])", r"\1_\2", tb_parent).lower().strip(), pk_)
                f_f_key = 'ForeignKey(%s, onupdate="cascade", ondelete="cascade")' % foreign_relation
                f_nullable = ''
                break
              if pk_ == col_name:
                f_unique = 'unique=True'
                f_nullable = ''

          temp = ''
          stuff = [f_type, f_f_key, f_nullable, f_unique]
          for ii in range(0, len(stuff)):
            item = stuff[ii]
            temp += item
            if item:
              temp += ', '

          temp = temp.strip()
          if temp[-1] == ',':
            temp = temp[:-1]
          
          if col_name == 'ID':
            a= 1
          new_col_name = normal_field(col_name)
          interface0 += "\t%s = Column(%s)\n" % (new_col_name, temp)

    interface0 += "\n\tviews = Column(Text)\n\n"
    interface0 += "\tcreated_user = Column(Unicode(50), ForeignKey('users.username', onupdate='cascade', ondelete='cascade'))\n"
    interface0 += "\tlast_edited_user = Column(Unicode(50), ForeignKey('users.username', onupdate='cascade', ondelete='cascade'))\n"
    interface0 += "\tcreated_date = Column(DateTime(timezone=False))\n"
    interface0 += "\tlast_edited_date = Column(DateTime(timezone=False))\n"

    if len(relationships):
      for relationship in relationships:
        relation_string = "\t%s = relationship(\n\t\t'%s',\n\t\tcascade=False,\n\t\tback_populates='%s')\n\n"
        if relationship['from'] == tb_name:
          relation_name = '_%s' % relationship['to']
          relation_to =  relationship['to'].title()
          relation_from = '_%s' % relationship['from']
        else:
          relation_name = '_%s' % relationship['from']
          relation_to =  relationship['from'].title()
          relation_from = '_%s' % relationship['to']
        relation_name = re.sub(r"(\w)([A-Z])", r"\1_\2", relation_name).lower().strip()
        relation_from = re.sub(r"(\w)([A-Z])", r"\1_\2", relation_from).lower().strip()
        interface_relations += relation_string % (relation_name, relation_to, relation_from)

    interface0 += '\n' + interface_relations
    interface += interface0

  interface = interface_header + interface

  file_path = __path__ + "\\" + result_name
  export_file(interface, file_path)

def create_layer_part(all_tables, all_groups, __path__):
  result_name = 'layer_part.py'
  
  # layer_parts = {'point':{}, 'line': {}, 'polygon':{}}
  layer_parts = {}
  # tmp = []
  for table in all_tables:
    tb_type = table['DatasetType'].replace('esriDT', '')
    tb_name = table['Name'].split('.')[-1]
    tb_alias = table['AliasName']
    tb_group = find_tb_group(table['CatalogPath'])

    # if tb_group not in tmp:
    #   tmp.append(tb_group)

    if tb_type not in ['Table', 'FeatureClass']:
      continue

    _name = tb_name
    _alias = tb_alias
    _group = ''
    _description = ''

    if tb_type == 'Table':
      _schema = 'api'
      _group = u'tables'
      add_layer_string = 'Layers(name="%s", alias=u"%s", schema="%s", zindex=__ZINDEX__, opacity=0, group=u"__GROUPNAME__", description=u"%s")'
    if tb_type == 'FeatureClass':
      _workspace = 'wrm'
      _datastore = 'wrm_api'
      _group = u'unknown'
      add_layer_string = 'Layers(name="%s", alias=u"%s", workspace="%s", datastore="%s", geometry_type="%s", dimension=%s, url="", type="geoserver", zindex=__ZINDEX__, opacity=80, group=u"__GROUPNAME__", description=u"%s")'
      tb_columns = table['Fields']['FieldArray']['Field']
      for column in tb_columns:
        col_type = column['Type'].replace('esriFieldType','')
        
        if col_type == 'Geometry':
          geometry_definition = column['GeometryDef']
          _geometry = geometry_definition['GeometryType'].replace('esriGeometry','').upper()
          
          if _geometry == 'POLYLINE':
            _geometry = 'MULTILINESTRING'
          
          _dimension = 2
          if geometry_definition['HasM'] == 'true':
            _dimension += 1
          if geometry_definition['HasZ'] == 'true':
            _dimension += 1

          break

    group_names = all_groups.keys()
    for group_name in group_names:
      group_items = all_groups[group_name]
      for group_item in group_items:
        if group_item == _name:
          _group = group_name

    if tb_type == 'Table':
      if _group not in layer_parts.keys():
        layer_parts[_group] = []
      layer_parts[_group].append( add_layer_string % (re.sub(r"(\w)([A-Z])", r"\1_\2", _name).lower().strip(), _alias, _schema, _description))
    if tb_type == 'FeatureClass':
      if _group not in layer_parts.keys():
        layer_parts[_group] = {}
      if _geometry not in layer_parts[_group].keys():
        layer_parts[_group][_geometry] = []
      layer_parts[_group][_geometry].append( add_layer_string % (re.sub(r"(\w)([A-Z])", r"\1_\2", _name).lower().strip(), _alias, _workspace, _datastore, _geometry, _dimension, _description))


  group_names = layer_parts.keys()

  _count = 0
  layer_part_string = ''
  groups_dictionary = {
    'tables': 'جداول',
    'baseLayers':'اطلاعات کشوری',
    'basics':'پایه',
    'facilities':'سد و تاسیسات آبی',
    'hydrology':'هیدرولوژی',
    'hydrogeology':'هیدروژئولوژی',
    'informatics':'فناوری اصلاعات',
    'projects':'طرح و پروژه ها',
    'rivers':'مهندسی رودخانه',
    'quality':'محیط زیست و کیفیت منابع آب',
    'unknown':'سایر'
  }

  for group_name in group_names:
    for k,v in groups_dictionary.items():
      if k == group_name:
        group_name_fa = v.decode("utf-8")
    layer_part_string += '\n# - - - - %s MXD - - - - #\n' % group_name.upper()
    group_items = layer_parts[group_name]
    if group_name == 'tables':
      for value in group_items:
        _count += 1
        value = value.replace('__ZINDEX__', str(_count))
        value = value.replace('__GROUPNAME__', group_name_fa)
        layer_part_string += value + ',\n'
    else:
      for feature_type in ['POINT', 'MULTILINESTRING', 'POLYGON']:
        for key, values in group_items.items():
          if key == feature_type:
            layer_part_string += '# %s\n' % feature_type
            for value in values:
              _count += 1
              value = value.replace('__ZINDEX__', str(_count))
              value = value.replace('__GROUPNAME__', group_name_fa)
              layer_part_string += value + ',\n'

  file_path = __path__ + "//" + result_name
  export_file(layer_part_string, file_path)

def create_add_shapefiles_part(target, __path__):
  result_name = 'add_shapfile_part.py'
  shapefiles = [glob.glob(f) for f in [target + '/*.shp']]
  shapefiles = shapefiles[0]

  add_shapfile_part = ''
  for shapefile in shapefiles:
    shapefile_path = shapefile.lower()
    shapefile_name = path.split(shapefile_path)[1].replace('.shp', '').title()
    add_shapfile_part += 'addShapeFile(r"%s",r"%s",session)\n' % (shapefile_path, shapefile_name)

  file_path = __path__ + "//" + result_name
  export_file(add_shapfile_part, file_path)
      
def main(__path__):
  xml_obj = parse_xml(__path__)
  
  data_elements = xml_obj['esri:Workspace']['WorkspaceDefinition']['DatasetDefinitions']['DataElement']
  tables = []
  relations = []
  groups = {}
  for data_element in data_elements:
    data_element_type = data_element['@xsi:type']
    if data_element_type in ['esri:DETable', 'esri:DEFeatureClass']:
      tables.append(data_element)
    elif data_element_type == 'esri:DERelationshipClass':
      relations.append(data_element)
    elif data_element_type == 'esri:DEFeatureDataset':
      group_name = data_element['Name'].split('.')[-1]
      for data_element_sub in data_element['Children']['DataElement']:
        data_element_sub_type = data_element_sub['@xsi:type']
        if data_element_sub_type == 'esri:DEFeatureClass':
          tables.append(data_element_sub)
          if group_name not in groups.keys():
            groups[group_name] = []
          groups[group_name].append(data_element_sub['Name'].split('.')[-1])
        elif data_element_sub_type == 'esri:DERelationshipClass':
          relations.append(data_element_sub)
  
  export_dir, export_sub_dir = path.split(__path__)
  export_dir = path.abspath(path.join(__path__, os.pardir)).replace('\\schema','\\') + export_sub_dir.replace('.xml', '') + ' (' +  now_date_time() + ')'
  
  if not file_exist(export_dir):
    os.makedirs(export_dir)
  
  create_dictionary(tables, export_dir)
  create_interfaces(tables, relations, export_dir)
  create_layer_part(tables, groups, export_dir)
  # create_add_shapefiles_part(r'C:\Python27\python271\api_ioptcv2\data\shp', export_dir)
  
  print '\n'
  print '--------------------------------------------------------------------'
  print 'Process Completed Successfully.'
  print '--------------------------------------------------------------------'
  print 'File were created successfully:'
  print '--------------------------------------------------------------------'
  print '%s\dictionary.js' % export_dir 
  print '%s\db_interface.py' % export_dir 
  print '%s\layer_part.py' % export_dir 
  print '%s\?' % export_dir 
  print '--------------------------------------------------------------------'
  print 'Done.'
  print '\n'


if __name__ == "__main__":
  # schema_name = 'file_name'
  schema_name = 'WRM_SCHEMA_13991134'
  schema_path = path.dirname(os.path.realpath('convertor.py')) + '\schema\%s.xml' % schema_name
  main(schema_path)