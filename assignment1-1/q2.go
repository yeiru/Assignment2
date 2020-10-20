package cos418_hw1_1

import (
	"bufio"
	"io"
	"strconv"
	"sync"
	"os"
)

var wg = sync.WaitGroup{}
// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`
	var sum int
	defer wg.Done()
	counterA:= len(nums)
	for i := 0; i<counterA; i++ {
		element := <- nums
		sum+=element
	}
	out <- sum
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// TODO: implement me
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers
	var sumA int
	var cut int
	var listIntB []int
	var r io.Reader
	f, err := os.Open(fileName)
	if err != nil {
		checkError(err)
	}
	defer f.Close()
	r = f
	listInt, err:=readInts(r)
 	if len(listInt) == 0 && err != nil {
		checkError(err)
	}
	nums := make(chan int, len(listInt))
	out := make(chan int, len(listInt))
	cut=len(listInt)/num
	for x := 1; x<=num; x++ {
		if len(listInt) <= num {
			cut = len(listInt)
			x=num+1
		}
		if len(listInt) > num && x == num{
			cut = len(listInt)
		}
		for _, v:=range listInt[:cut] {
			nums <- v
		}
		wg.Add(1)
		go sumWorker(nums, out)
		wg.Wait()
		listIntB=listInt[cut:]
		listInt=listIntB
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		counter:= len(out)
		for y := 0; y<counter; y++ {
			elementA := <- out
			sumA+=elementA
		}
	}()
	wg.Wait()
	return sumA
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
