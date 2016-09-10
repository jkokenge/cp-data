#!/usr/bin/env python
# -*- coding: utf-8 -*-
 
"""
Description: extract email addresses from stdin to stdout
Usage example: cat whatever.txt | extract-email.py | sort -fu > addr.txt
"""
 
import re, sys
 
email_pattern = re.compile('([\w\-\.]+@(\w[\w\-]+\.)+[\w\-]+)', re.MULTILINE | re.IGNORECASE)

emails = re.findall(email_pattern, test_str)
print(emails)