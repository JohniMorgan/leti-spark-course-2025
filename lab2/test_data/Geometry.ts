class Vector2DModified {
  constructor(public a: number, public b: number) {}

  sum(vec: Vector2DModified): Vector2DModified {
    return new Vector2DModified(this.a + vec.a, this.b + vec.b);
  }

  length(): number {
    return Math.hypot(this.a, this.b);
  }
}

const vect = new Vector2DModified(3, 4);
console.log(vect.length());