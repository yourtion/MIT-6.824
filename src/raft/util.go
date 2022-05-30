package raft

import "log"

// Debug Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
