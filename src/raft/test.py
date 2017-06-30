#!/usr/bin/env python
# coding: utf-8

import os
import sys

n = 10

if len(sys.argv) >= 2:
    n = int(sys.argv[1])

print "TestInitialElection"
for i in range(n):
    os.system("go test raft -run=TestInitialElection")

print "TestReElection"
for i in range(n):
    os.system("go test raft -run=TestReElection")

print "TestBasicAgree"
for i in range(n):
    os.system("go test raft -run=TestBasicAgree")

print "TestFailAgree"
for i in range(n):
    os.system("go test raft -run=TestFailAgree")

print "TestFailNoAgree"
for i in range(n):
    os.system("go test raft -run=TestFailNoAgree")

print "TestConcurrentStarts"
for i in range(n):
    os.system("go test raft -run=TestConcurrentStarts")

print "TestRejoin"
for i in range(n):
    os.system("go test raft -run=TestRejoin")

print "TestBackup"
for i in range(n):
    os.system("go test raft -run=TestBackup")

print "TestCount"
for i in range(n):
    os.system("go test raft -run=TestCount")

print "TestPersist1"
for i in range(n):
    os.system("go test raft -run=TestPersist1")

print "TestPersist2"
for i in range(n):
    os.system("go test raft -run=TestPersist2")

print "TestPersist3"
for i in range(n):
    os.system("go test raft -run=TestPersist3")

print "TestFigure8"
for i in range(n):
    os.system("go test raft -run=TestFigure8$")

print "TestUnreliableAgree"
for i in range(n):
    os.system("go test raft -run=TestUnreliableAgree")

print "TestFigure8Unreliable"
for i in range(n):
    os.system("go test raft -run=TestFigure8Unreliable")

print "TestReliableChurn"
for i in range(n):
    os.system("go test raft -run=TestReliableChurn")

print "TestUnreliableChurn"
for i in range(n):
    os.system("go test raft -run=TestUnreliableChurn")
