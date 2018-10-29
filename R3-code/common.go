package main

type JoinGameRequest struct {
	GameName string
	Player   PublicPlayer
}

type JoinGameReply struct {
	Height        int
	Width         int
	Ships         []Ship
	PublicPlayers []PublicPlayer
}

type Ship struct {
	Name      string
	Locations []Location
}

type Location struct {
	Row    int
	Column int
}

type PublicPlayer struct {
	Name      string
	Port      string
	IPAddress string
}

type Player struct {
	Name      string
	IPAddress string
	Port      string
	Ships     []Ship
}

type AttackRequest struct {
	Target Location
}

type AttackReply struct {
	Success bool
}
