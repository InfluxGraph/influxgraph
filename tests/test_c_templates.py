import unittest
from influxgraph.ext.templates import parse_series
from string import ascii_letters
from random import choice
from influxgraph.templates import _parse_influxdb_graphite_templates

class TemplatesCExtTestCase(unittest.TestCase):

    def setUp(self):
        self.metric_prefix = u"template_integration_test"
        self.paths = [u'test_type', u'host']
        self.measurements = ['cpu', 'memory', 'load', 'iops']
        self.field_names = [u'field1', u'field2']
        self.all_fields = {m: self.field_names for m in self.measurements}
        self.tags = {
            # Tags parsed from metric path
            self.paths[0]: self.metric_prefix,
            self.paths[1]: 'localhost',
            # Default tags not in metric path
            'env': u'int',
            'region': u'the_west',
            }
        template = "%s %s.measurement.field* env=int,region=the_west" % (
            self.metric_prefix, ".".join([p for p in self.paths]))
        self.templates = _parse_influxdb_graphite_templates([template])
        self.series = [u'%s,test_type=%s,host=%s,env=%s,region=%s' % (
            m, self.tags[self.paths[0]], self.tags[self.paths[1]],
            self.tags['env'], self.tags['region'],) for m in self.measurements]
        self.graphite_series = ["%s" % (".".join(
            [self.tags[p] for p in self.paths] + [m])) for m in self.measurements]

    def test_template_parse(self):
        measurements = [''.join([choice(ascii_letters) for _ in range(10)]) for _ in range(10)]
        series = [u'%s,a=1,b=2,c=3,d=4,e=5,f=6,g=7,m=8,n=9,j=10' % (m,) for m in measurements]
        field_names = [u'f1', u'f2', u'f3', u'f4', u'f5', u'f6', u'f7', u'f8', u'f9', u'f10']
        fields = {m: [u'f1', u'f2', u'f3', u'f4', u'f5', u'f6', u'f7', u'f8', u'f9', u'f10'] for m in measurements}
        templates = _parse_influxdb_graphite_templates(
            ["a.b.c.d.e.f.g.m.n.j.measurement.field* env=int,region=the_west"])
        index = parse_series(series, fields, templates)
        self.assertTrue(index is not None)
        self.assertEqual([n['metric'] for n in index.query('*')], [u'1'])
        self.assertEqual([n['metric'] for n in index.query('*.*')], [u'1.2'])
        self.assertEqual([n['metric'] for n in index.query('*.*.*')], [u'1.2.3'])
        self.assertEqual([n['metric'] for n in index.query('*.*.*.*')], [u'1.2.3.4'])
        self.assertEqual([n['metric'] for n in index.query('*.*.*.*.*')], [u'1.2.3.4.5'])
        self.assertEqual([n['metric'] for n in index.query('*.*.*.*.*.*')], [u'1.2.3.4.5.6'])
        self.assertEqual([n['metric'] for n in index.query('*.*.*.*.*.*.*')], [u'1.2.3.4.5.6.7'])
        self.assertEqual([n['metric'] for n in index.query('*.*.*.*.*.*.*.*')], [u'1.2.3.4.5.6.7.8'])
        self.assertEqual([n['metric'] for n in index.query('*.*.*.*.*.*.*.*.*')], [u'1.2.3.4.5.6.7.8.9'])
        self.assertEqual([n['metric'] for n in index.query('*.*.*.*.*.*.*.*.*.*')], [u'1.2.3.4.5.6.7.8.9.10'])
        self.assertEqual(sorted([n['metric'] for n in index.query('*.*.*.*.*.*.*.*.*.*.%s.*' % (measurements[0],))]),
                         sorted([u'1.2.3.4.5.6.7.8.9.10.%s.%s' % (measurements[0], f,) for f in field_names]))

    def test_templated_index_find(self):
        index = parse_series(self.series, self.all_fields, self.templates)
        query = '*'
        nodes = [n['metric'] for n in index.query(query)]
        expected = [self.metric_prefix]
        self.assertEqual(nodes, expected,
                         msg="Got root branch query result %s - wanted %s" % (
                             nodes, expected,))
        query = str("%s.*" % (self.metric_prefix,))
        nodes = [n['metric'] for n in index.query(query)]
        expected = ['.'.join([self.metric_prefix,
                             self.tags[self.paths[1]]])]
        self.assertEqual(nodes, expected,
                         msg="Got sub branch query result %s - wanted %s" % (
                             nodes, expected,))
        query = str("%s.%s.*" % (self.metric_prefix, self.tags[self.paths[1]]))
        nodes = sorted([n['metric'] for n in index.query(query)])
        expected = sorted(['.'.join([self.metric_prefix,
                                     self.tags[self.paths[1]],
                                     m]) for m in self.measurements])
        self.assertEqual(nodes, expected,
                         msg="Got sub branch query result %s - wanted %s" % (
                             nodes, expected,))
        query = str("%s.%s.*.*" % (self.metric_prefix, self.tags[self.paths[1]],))
        nodes = sorted([n['metric'] for n in index.query(query)])
        expected = sorted(['.'.join([self.metric_prefix,
                                     self.tags[self.paths[1]],
                                     m, f])
                                     for m in self.measurements
                                     for f in self.field_names])
        self.assertEqual(nodes, expected,
                         msg="Got fields query result %s - wanted %s" % (
                             nodes, expected,))
