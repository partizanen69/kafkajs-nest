import * as fs from 'fs';

(async () => {
  const content: string = await fs.promises.readFile('./check.completenes.txt', {
    encoding: 'utf8',
  });

  const sequence: number[] = content.split(',')
    .map((item: string) => +item.split('-')[1].trim())
    .sort((a: number, b: number) => {
      return +a > +b ? 1 : -1;
    });

  if (!sequence.length) {
    throw new Error(`No items in the file`);
  }
  
  let prev: number = sequence[0];
  for (let i = 1; i < sequence.length; i++) {
    if (prev + 1 !== sequence[i]) {
      console.log(
        'Sorted sequence: ', 
        sequence,
        `Sequence is not sequential`,
        prev,
        sequence[i],
      );
      return;
    }
    prev = i;
  }

  console.log(`Test passed. Sequence is complete.`);
})();