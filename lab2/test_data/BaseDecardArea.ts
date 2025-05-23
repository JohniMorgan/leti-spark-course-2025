interface Point {
  x: number;
  y: number;
}

const vectorOperations = {
  add(p1: Point, p2: Point): Point {
    return { x: p1.x + p2.x, y: p1.y + p2.y };
  },
  norm(p: Point): number {
    return (p.x ** 2 + p.y ** 2) ** 0.5;
  }
};

console.log(vectorOperations.norm({x:3, y:4}));