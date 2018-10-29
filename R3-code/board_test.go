package main

import "testing"

func TestEmptyBoard(t *testing.T) {

	height := 12
	width := 13

	emptyBoard := EmptyBoard(height, width)

	var maxRow int
	var maxColumn int

	for r, row := range emptyBoard {
		maxRow = r
		for c, cell := range row {
			maxColumn = c
			if cell != empty {
				t.Errorf("Found non-empty cell in row %v, column %v", r, c)
			}
		}
	}

	if maxRow != height-1 {
		t.Errorf("Number of rows sohuld be %v, not %v", height-1, maxRow)
	}

	if maxColumn != width-1 {
		t.Errorf("Number of column sohuld be %v, not %v", width-1, maxColumn)
	}
}

func TestAddShips(t *testing.T) {

	height := 5
	width := 5

	board := EmptyBoard(height, width)

	shipLocations := []Location{
		Location{Row: 1, Column: 1},
		Location{Row: 1, Column: 2},
	}

	ships := []Ship{
		Ship{
			Name:      "ship name",
			Locations: shipLocations,
		},
	}

	board.AddShips(ships)

	for r, row := range board {
		for c, cell := range row {
			if r == 1 && (c == 1 || c == 2) {
				if cell != ship {
					t.Errorf("Missing ship in row %v, and column %v", r, c)
				}
			} else {
				if cell != empty {
					t.Errorf("Found non-empty cell in row %v, column %v", r, c)
				}
			}
		}
	}
}

func TestAttack(t *testing.T) {

	height := 5
	width := 5

	board := EmptyBoard(height, width)

	shipLocations := []Location{
		Location{Row: 1, Column: 1},
		Location{Row: 1, Column: 2},
	}

	ships := []Ship{
		Ship{
			Name:      "ship name",
			Locations: shipLocations,
		},
	}

	board.AddShips(ships)

	// Check error if target outside boundary
	target := Location{6, 1}
	hitShip, err := board.Attack(target)
	if err == nil {
		t.Errorf("Able to attack outside boundary with row %v, column %v", target.Row, target.Column)
	}

	// Check missing a ship: no error, return false, mutates cell to miss
	target = Location{2, 1}
	hitShip, err = board.Attack(target)
	if err != nil {
		t.Errorf("Threw error with row %v, column %v", target.Row, target.Column)
	}
	if hitShip {
		t.Errorf("Attack empty cell should return false: row %v, column %v", target.Row, target.Column)
	}
	if board[target.Row][target.Column] != miss {
		t.Errorf("Missed attack should update cell to miss: row %v, column %v", target.Row, target.Column)
	}

	// Check attacking a ship: no error, return false, mutates cell to hitSelf
	target = Location{1, 1}
	hitShip, err = board.Attack(target)
	if err != nil {
		t.Errorf("Threw error with row %v, column %v", target.Row, target.Column)
	}
	if !hitShip {
		t.Errorf("Attack ship cell should return true: row %v, column %v", target.Row, target.Column)
	}
	if board[target.Row][target.Column] != hitSelf {
		t.Errorf("Successful attack should update cell to hitSelf: row %v, column %v", target.Row, target.Column)
	}
}
