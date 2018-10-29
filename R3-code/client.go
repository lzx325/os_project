package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

/* ---------- Global Variables ---------- */

var localBoard Board
var mu sync.Mutex
var SERVER_ADDRESS string
var SERVER_PORT string
var PLAYER_PORT string
var GAME_NAME string
var PLAYER_NAME string
var please_go = make(chan int)

func main() {

	/* ---------- NOTE: Start reading here ---------- */
	parseArgument()

	/* ---------- TASK 1 and 2: Implement the 2 function to join a game ---------- */
	server := establishConnection(SERVER_ADDRESS, SERVER_PORT)
	reply := joinGame(server, PLAYER_NAME, GAME_NAME, PLAYER_PORT)
	fmt.Println("Joined Game")

	/* ---------- NOTE: Board will be created for you. Nothing to implement here  ---------- */
	localBoard = EmptyBoard(reply.Height, reply.Width)
	localBoard.AddShips(reply.Ships)

	/* ---------- TASK 3: Implement an Attack Server to allow your opponent to attack you ---------- */
	startAttackServer()

	/* ---------- NOTE: Connection to your opponent. Nothing to implement here  ---------- */
	opponent, yourTurn := connetToOppnent(reply.PublicPlayers)
	if yourTurn {
		fmt.Println("----- You will go first")
		printBoard()
	}

	/* ---------- TASK 4: Add logic to control taking turns ---------- */

	// Main loop

	for {
		if yourTurn {
			// Get user input, and attack opponent
			target := getTarget()
			attack(opponent, target)
			yourTurn = !yourTurn
		} else {
			printBoard()
			fmt.Println("----- Waiting for opponent")
			<-please_go
			yourTurn = !yourTurn
		}

	}
}

/**
 * Given a serverAddress (ip address) and port,
 * returns a client that can be used to call the server
 * See the rpc.DialHTTP example at: https://golang.org/pkg/net/rpc/
 * Note: don't forget to add ":" between the address and port
 */
func establishConnection(serverAddress, port string) *rpc.Client {

	/* ----- TASK 1: create a connection to the server ----- */
	client, err := rpc.DialHTTP("tcp", fmt.Sprint(serverAddress, ":", port))
	if err != nil {
		log.Fatal("dialing:", err)
	}

	return client

}

/**
 * Calls the BattleshipsService.JoinGame function on the server to join a game
 * The server response with:
 * - Information about the other players (name, IP, port, etc)
 * - The height and width of the board
 * - The location of your ships
 * See common.go for more details
 */
func joinGame(client *rpc.Client, playerName string, gameName string, port string) JoinGameReply {

	ipAddress := getOutboundIP().String()
	fmt.Println("----- Your IP Address:", ipAddress)

	/* ----- TASK 2: join a game ----- */

	/**
	 * Hint 1: the function you are calling on the server has the following signature:
	 * func (t *BattleshipsService) JoinGame(request JoinGameRequest, reply *JoinGameReply) error
	 *
	 * Hint 2: See common.go
	 */
	args := &JoinGameRequest{gameName, PublicPlayer{playerName, port, ipAddress}}
	var reply JoinGameReply
	err := client.Call("BattleshipsService.JoinGame", args, &reply)
	if err != nil {
		log.Fatal("Connection error：", err)
	}
	return reply

}

type PlayerToPlayerService int

/**
 * Start an attack server
 * An opponent should be able to call the Attack(...) function bellow through an RPC
 * The service name for Attack(...) is PlayerToPlayerService (defined above)
 * See the rpc example for rpc.Register and rpc.HandleHTTP on: https://golang.org/pkg/net/rpc/
 */
func startAttackServer() {

	/* ---------- TASK 3: Implement an Attack Server to allow your opponent to attack you ---------- */
	playerToPlayerService := new(PlayerToPlayerService)
	rpc.Register(playerToPlayerService)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+PLAYER_PORT)
	if err != nil {
		log.Fatal("Listen err: ", err)
	}
	fmt.Println("Listening on port", PLAYER_PORT)
	go http.Serve(listener, nil)

	// arith := new(PlayerToPlayerService)
	// rpc.Register(arith)
	// rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":20000")
	// if e != nil {
	// 	log.Fatal("listen error:", e)
	// }
	// go http.Serve(l, nil)
}

/**
 * When an opponent calls Attack through an RPC, the local board is updated with this attack
 */
func (t *PlayerToPlayerService) Attack(request AttackRequest, reply *AttackReply) error {
	mu.Lock()
	defer mu.Unlock()

	reply.Success, _ = localBoard.Attack(request.Target)

	if reply.Success {
		fmt.Printf("----- Hit by opponent at ( %v , %v )\n", request.Target.Row, request.Target.Column)
	} else {
		fmt.Printf("-----  Opponent missed at ( %v , %v )\n", request.Target.Row, request.Target.Column)
	}

	printBoard()
	please_go <- 1

	/* ---------- TASK 4 Hint 1: May need logic to control taking turns ---------- */
	/* ---------- TASK 4 Hint 2: This is running in a different go routine than main ---------- */

	return nil
}

/* ------------------------------------------------------------- */
/* ---------- NOTE: You don't need to read below this ---------- */
/* ------------------------------------------------------------- */

/**
 * Given an opponent and a target location,
 * calls the opponent's Attack rpc using
 * AttackRequest and expects an AttackReply
 * If the attack was successful (a ship is in that location),
 * AttackReply.Success will be true
 */
func attack(opponent *rpc.Client, target Location) {

	var reply AttackReply
	request := AttackRequest{
		Target: target,
	}

	err := opponent.Call("PlayerToPlayerService.Attack", request, &reply)
	if err != nil {
		log.Fatal("Error attacking player")
	}
	if reply.Success {
		localBoard.UpdateHit(target)
		fmt.Println("----- HIT!")
	} else {
		localBoard.UpdateMiss(target)
		fmt.Println("----- MISS")
	}
}

/**
 * Get a row and column from the user and return a location on the board
 * Will repeat the prompt if the location chosen is:
 * - Outside the board
 * - Already hit by self or an opponent
 * - Contains a friendly ship
 */
func getTarget() Location {
	fmt.Println("----- Your move")
	var targetRow int
	var targetColumn int

	err := errors.New("Invalid input")
	for err != nil {
		fmt.Print("\tEnter a row and column (e.g. '3 4') then press enter:")
		_, err = fmt.Scan(&targetRow, &targetColumn)
		fmt.Print("\n")

		var cell Cell
		cell, err = localBoard.Get(targetRow, targetColumn)

		if err != nil {
			fmt.Println("\tWe can't shoot that far. Be sure to target within the board")
			continue
		}

		if cell == ship {
			fmt.Println("\tBomb the enemy ships not ours you dingus!")
			err = errors.New("Invalid input")
		}

		if cell == hitSelf || cell == hitOther {
			fmt.Println("\tI think they're already dead")
			err = errors.New("Invalid input")
		}

		if cell == miss {
			fmt.Println("\tWe know nothing is there")
			err = errors.New("Invalid input")
		}
	}
	return Location{targetRow, targetColumn}
}

func printBoard() {
	fmt.Println("----- BATTLE FIELD\n")
	fmt.Print(localBoard.String())
}

func connetToOppnent(players []PublicPlayer) (*rpc.Client, bool) {
	myIP := getOutboundIP()
	var opponent *rpc.Client
	yourTurn := false
	for playerIdx := 0; playerIdx < len(players); playerIdx++ {
		playerName := players[playerIdx].Name
		curIP := players[playerIdx].IPAddress
		curPort := players[playerIdx].Port

		if (curIP == myIP.String()) && (curPort == PLAYER_PORT) {
			if playerIdx == 0 {
				yourTurn = true
			}
		} else {
			fmt.Println("\tConnecting to:", playerName, "at address:", curIP, "and port:", curPort)
			opponent = establishConnection(curIP, curPort)
		}
	}
	fmt.Println("----- Connected to opponent")
	return opponent, yourTurn
}

/**
 * Returns this computer's IP Address
 * Source:
 * https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
 */
func getOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP
}

/**
 * Parse command line arguemnts:
 * GameName PlayerName ServerAddress ServerPort PlayerPort
 *
 * Example:
 * game_name player_name 10.85.37.199 6001 9831
 */
func parseArgument() {
	if len(os.Args) != 6 {
		log.Fatal("Usage:", os.Args[0], " GameName PlayerName ServerAddress ServerPort PlayerPort")
	}
	GAME_NAME = os.Args[1]
	PLAYER_NAME = os.Args[2]
	SERVER_ADDRESS = os.Args[3]
	SERVER_PORT = os.Args[4]
	PLAYER_PORT = os.Args[5]
}
