type Coord = [number, number];

class VectorCalculator {
  static add([x1,y1]: Coord, [x2,y2]: Coord): Coord {
    return [x1+x2, y1+y2];
  }
  
  static magnitude([x,y]: Coord): number {
    return Math.sqrt(x*x + y*y);
  }
}

console.log(VectorCalculator.magnitude([3,4]));