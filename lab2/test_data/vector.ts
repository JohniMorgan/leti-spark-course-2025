class Vector {
  constructor(private coordinates: [number, number]) {}

  get magnitude(): number {
    const [x,y] = this.coordinates;
    return Math.hypot(x,y);
  }
  
  combine(other: Vector): Vector {
    return new Vector([
      this.coordinates[0] + other.coordinates[0],
      this.coordinates[1] + other.coordinates[1]
    ]);
  }
}

const vec = new Vector([3,4]);
console.log(vec.magnitude);