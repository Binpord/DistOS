package main

import (
	"fmt"
	"world"
)

func workFunction_ATTN(m *world.Message, p *world.Process, w *world.World) bool {
	s := m.GetString()
	if !p.IsMyMessage("ATTN", s) || s == "*TIME" {
		return false
	}

	neibs := w.nl.GetNeibs(p.node)
	if s == "ATTN_INIT" {
		arg := m.GetInt32()
		fmt.Printf("[%v]: ATTN_INIT %v received\n", p.node, arg)
		for _, neib := range neibs {
			new_m = world.NewMessage(p.node, neib)
			new_m.Append(world.NewMessageArg(arg))
			w.nl.messages <- new_m
		}
	}
}

func main() {
}
