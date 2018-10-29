package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

const (
	PORT         string = "6001"
	NUM_PLAYERS  int    = 2
	BOARD_HEIGHT int    = 10
	BOARD_WIDTH  int    = 10
)

var GAMES map[string]Game = make(map[string]Game) // Games names to games
var gamesMu sync.Mutex

type BattleshipsService int

type Game struct {
	Name     string
	Wg       *sync.WaitGroup
	Players  []Player
	ShipMaps [][]Ship
}

func main() {
	// Setup the service and listen for any requests on the port
	battleshipService := new(BattleshipsService)
	rpc.Register(battleshipService)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+PORT)
	if err != nil {
		log.Fatal("Listen err: ", err)
	}
	fmt.Println("Listening on port:", PORT)
	http.Serve(listener, nil)
}

func (t *BattleshipsService) JoinGame(request JoinGameRequest, reply *JoinGameReply) error {

	gameName := request.GameName
	thisPlayer := request.Player

	ships, allPlayers := addPlayer(gameName, thisPlayer)

	reply.Height = BOARD_HEIGHT
	reply.Width = BOARD_WIDTH
	reply.Ships = ships
	reply.PublicPlayers = allPlayers

	return nil
}

/**
 * Creates a new game if gameName is not in GAMES, and adds the player to it.
 * If game exists, adds player to it.
 * Blocks until enough people join gameName
 *
 * Returns the ships of the player that wants to join,
 * and the public information of other players in the game
 */
func addPlayer(gameName string, publicPlayer PublicPlayer) ([]Ship, []PublicPlayer) {

	// gamesMu.Lock()

	// If game doesn't exist, make it
	game, gameExists := GAMES[gameName]

	if !gameExists {
		var wg sync.WaitGroup
		wg.Add(NUM_PLAYERS)
		shipMaps := getRandomShipLocations()
		game = Game{
			Name:     gameName,
			Wg:       &wg,
			Players:  []Player{},
			ShipMaps: shipMaps,
		}
		GAMES[gameName] = game
	}

	// gamesMu.Unlock()

	// Place ships
	var ships []Ship
	shipMaps := game.ShipMaps
	if len(game.Players) == 1 {
		ships = shipMaps[0]
	} else {
		ships = shipMaps[1]
	}

	// Create player with ships
	thisPlayer := Player{
		Name:      publicPlayer.Name,
		IPAddress: publicPlayer.IPAddress,
		Port:      publicPlayer.Port,
		Ships:     ships,
	}

	// Add player and ships to game
	game.Players = append(game.Players, thisPlayer)
	GAMES[gameName] = game

	fmt.Println("GAMES: ", GAMES)

	game.Wg.Done()
	game.Wg.Wait()

	// gamesMu.Lock()

	game = GAMES[gameName]
	players := game.Players

	publicPlayers := make([]PublicPlayer, len(players))
	for i, player := range players {
		publicPlayers[i] = PublicPlayer{
			Name:      player.Name,
			IPAddress: player.IPAddress,
			Port:      player.Port,
		}
	}

	time.Sleep(2000 * time.Millisecond)

	if game, exists := GAMES[gameName]; exists {
		delete(GAMES, gameName)

		player1 := game.Players[0].Name
		players := game.Players[1].Name

		fmt.Println(player1, "vs", players, " -- FIGHT!")
	}

	// gamesMu.Unlock()

	return thisPlayer.Ships, publicPlayers
}
