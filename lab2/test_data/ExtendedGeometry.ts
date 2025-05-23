class Vector2DExt {
  constructor(public a: number, public b: number) {}

  sum(vec: Vector2DExt): Vector2DExt {
    return new Vector2DExt(this.a + vec.a, this.b + vec.b);
  }

  length(): number {
    return Math.hypot(this.a, this.b);
  }
}

const vectExt = new Vector2DExt(3, 4);
console.log(vect.length());

class Circle2DExt {
    constructor(public x: number, public y: number, public r: number ){}

    diametr() { return this.r * 2};

    area() {
        return Math.PI * this.r * this.r
    }

    translate(vec: Vector2DExt): Circle2DExt {
        this.x = vec.a;
        this.y = vec.b;
        return this;
    }
}