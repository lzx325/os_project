package main

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"text/tabwriter"
)

type Cell int

const (
	empty    Cell = 0
	ship     Cell = 1
	hitSelf  Cell = 2
	hitOther Cell = 3
	miss     Cell = 4
)

type Board [][]Cell

/* ---------- Public ---------- */

/**
 * @param height: 	height of board
 * @param width: 	width of board
 * @return:			An empy board of size height x width
 */
func EmptyBoard(height, width int) Board {
	board := make([][]Cell, height)
	for row := 0; row < height; row++ {
		board[row] = make([]Cell, width)
		for column := 0; column < width; column++ {
			board[row][column] = empty
		}
	}
	return board
}

/**
 * Adds ships to the board
 * Does not alter other cells
 */
func (board *Board) AddShips(ships []Ship) {

	for i := 0; i < len(ships); i++ {
		for j := 0; j < len(ships[i].Locations); j++ {
			row := ships[i].Locations[j].Row
			column := ships[i].Locations[j].Column
			(*board)[row][column] = ship
		}
	}
}

func (board *Board) Get(row, column int) (Cell, error) {
	target := Location{Row: row, Column: column}
	if board.locOutsideBoard(target) {
		return empty, errors.New("Row or column are outside playable board")
	}

	return (*board)[row][column], nil
}

/**
 * Simulates an opponent's attack
 * If the cell contains a ship, it becomes a hitSelf
 * If not, it becomes a miss
 *
 * @return: true if the cell contains ship or hit. Error if the target is outside the board
 */
func (board *Board) Attack(target Location) (bool, error) {
	if board.locOutsideBoard(target) {
		return false, errors.New("Target was outside board boundaries")
	}

	hitShip := false
	cell := (*board)[target.Row][target.Column]

	if cell == ship || cell == hitSelf {
		hitShip = true
	}

	if hitShip {
		(*board)[target.Row][target.Column] = hitSelf
	} else {
		(*board)[target.Row][target.Column] = miss
	}

	return hitShip, nil
}

func (board *Board) UpdateHit(target Location) {
	(*board)[target.Row][target.Column] = hitOther
}

func (board *Board) UpdateMiss(target Location) {
	(*board)[target.Row][target.Column] = miss
}

func (board *Board) String() string {

	if len(*board) == 0 {
		return "BOARD HAS SIZE ZERO -- NOT PRINTING\n"
	}

	width := len(*board)
	height := len((*board)[0])

	var buffer bytes.Buffer
	tabWriter := tabwriter.NewWriter(&buffer, 3, 0, 1, ' ', 0)

	// Add column numbers to first line
	fmt.Fprint(tabWriter, "\t")
	for column := 0; column < width; column++ {
		fmt.Fprint(tabWriter, strconv.Itoa(column)+"\t")
	}
	fmt.Fprint(tabWriter, "\n")

	// Populate row numbers and board itself
	for row := 0; row < height; row++ {
		fmt.Fprint(tabWriter, strconv.Itoa(row)+"\t")
		for column := 0; column < width; column++ {
			switch (*board)[row][column] {
			case ship:
				fmt.Fprint(tabWriter, "S\t")
			case hitOther:
				fmt.Fprint(tabWriter, "X\t")
			case hitSelf:
				fmt.Fprint(tabWriter, "!\t")
			case miss:
				fmt.Fprint(tabWriter, "O\t")
			default:
				fmt.Fprint(tabWriter, "~\t")
			}
		}
		fmt.Fprint(tabWriter, "\n")
	}
	tabWriter.Flush()
	return buffer.String()
}

/* ---------- Private ---------- */

/**
 * @return: true if target is outside the board
 */
func (board *Board) locOutsideBoard(target Location) bool {

	isOutsideBoard := target.Row >= len(*board) ||
		target.Column >= len((*board)[0]) ||
		target.Row < 0 ||
		target.Column < 0

	return isOutsideBoard
}
