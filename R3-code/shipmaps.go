package main
import (
	"math/rand"
	"strconv"
	"time"
)

const (
	numberOfPlayers = 2
	numberOfShipsPerPlayer = 2
	shipSize = 3
	numberRows = 10
	numberColumns = 10
)
var ShipMaps = getRandomShipLocations()
var random = rand.New(rand.NewSource(time.Now().UnixNano()))
func getRandomShipLocations() [][]Ship {

	var ShipMaps = make([][]Ship, numberOfPlayers)
	for player := 0; player < numberOfPlayers; player++ {
		ShipMaps[player] = make([]Ship, numberOfShipsPerPlayer)
		for ship := 0; ship < numberOfShipsPerPlayer; ship++ {
			ShipMaps[player][ship].Name = "ship"+strconv.Itoa(ship)
			ShipMaps[player][ship].Locations = make([]Location, shipSize)
			}
		}
	
	fillLocationsWithRandomLocation(ShipMaps)
	return ShipMaps
}


func fillLocationsWithRandomLocation(shipMaps [][]Ship) {
	for player := 0; player < numberOfPlayers; player++ {
		for ship := 0; ship < numberOfShipsPerPlayer; ship++ {
			var randomLocation []Location
			for {
				randomLocation = getRAndomLocation()
				if !overlapping(randomLocation, shipMaps) {
					break
				} 
			}
			shipMaps[player][ship].Locations = randomLocation
			}
		}
	}


func overlapping(randomLocation []Location, shipMaps [][]Ship) bool {
	for i := 0; i < len(randomLocation); i++ {
		if (randomLocation[i].Row < 0 || 
			randomLocation[i].Row >= numberRows ||
			randomLocation[i].Column < 0 || 
			randomLocation[i].Column >= numberColumns) {
			return true
		}
	} 
	for _, playerVal := range shipMaps {
		for _, shipVal := range playerVal {
			for _, locationVal := range shipVal.Locations {
				for _, curRandom := range randomLocation {
					if (locationVal.Row == curRandom.Row && 
						locationVal.Column == curRandom.Column) {
						return true
					}
				}
			}

		}
	}
	return false
}
func getRAndomLocation() []Location {
	cur_row := random.Intn(10)
	cur_col := random.Intn(10)
	locations := make([]Location, shipSize)
	locations[0] = Location{Row: cur_row, Column: cur_col}
	switch random.Intn(4) {
		case 0:
			for i := 1; i < shipSize; i++ {
				locations[i] = Location{Row: cur_row-i, Column: cur_col}
			}
		case 1: 
			for i := 1; i < shipSize; i++ {
				locations[i] = Location{Row: cur_row+i, Column: cur_col}
			}
		case 2:
			for i := 1; i < shipSize; i++ {
				locations[i] = Location{Row: cur_row, Column: cur_col-i}
			}
		case 3:
			for i := 1; i < shipSize; i++ {
				locations[i] = Location{Row: cur_row, Column: cur_col+i}
			}
	}
	return locations
}

