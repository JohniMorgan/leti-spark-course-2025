class Vector2D {
  constructor(public x: number, public y: number) {}

  add(vec: Vector2D): Vector2D {
    return new Vector2D(this.x + vec.x, this.y + vec.y);
  }

  magnitude(): number {
    return Math.sqrt(this.x ** 2 + this.y ** 2);
  }
}

const v1 = new Vector2D(3, 4);
console.log(v1.magnitude());