# coding=utf-8

"""
Collect data from S.M.A.R.T.'s attribute reporting.

#### Dependencies

 * [smartmontools](http://sourceforge.net/apps/trac/smartmontools/wiki)

"""

import diamond.collector
import subprocess
import re
import os


class SmartCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(SmartCollector, self).get_default_config_help()
        config_help.update({
            'device_dir': "device directory, e.g. /dev or /dev/disk/by-id",
            'devices': "device regex to collect stats on",
            'bin':         'The path to the smartctl binary',
            'use_sudo':    'Use sudo?',
            'sudo_cmd':    'Path to sudo',
        })
        return config_help

    def get_default_config(self):
        """
        Returns default configuration options.
        """
        config = super(SmartCollector, self).get_default_config()
        config.update({
            'path': 'smart',
            'bin': 'smartctl',
            'use_sudo':         False,
            'sudo_cmd':         '/usr/bin/sudo',
            'device_dir': '/dev',
            'devices': '^disk[0-9]$|^sd[a-z]$|^hd[a-z]$',
            'method': 'Threaded'
        })
        return config

    def collect(self):
        """
        Collect and publish S.M.A.R.T. attributes
        """
        device_dir = self.config['device_dir']
        devices = re.compile(self.config['devices'])

        for device in os.listdir(device_dir):
            if devices.match(device):

                command = [self.config['bin'], "-A", os.path.join(device_dir,
                                                                  device)]

                if self.config['use_sudo']:
                    command.insert(0, self.config['sudo_cmd'])

                attributes = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE
                ).communicate()[0].strip().splitlines()

                metrics = {}

                for attr in attributes[7:]:
                    attribute = attr.split()
                    if attribute[1] != "Unknown_Attribute":
                        metric = "%s.%s" % (device, attribute[1])
                    else:
                        metric = "%s.%s" % (device, attribute[0])

                    # New metric? Store it
                    if metric not in metrics:
                        metrics[metric] = attribute[9]
                    # Duplicate metric? Only store if it has a larger value
                    # This happens semi-often with the Temperature_Celsius
                    # attribute You will have a PASS/FAIL after the real temp,
                    # so only overwrite if The earlier one was a
                    # PASS/FAIL (0/1)
                    elif metrics[metric] == 0 and attribute[9] > 0:
                        metrics[metric] = attribute[9]
                    else:
                        continue

                for metric in metrics.keys():
                    self.publish(metric, metrics[metric])
