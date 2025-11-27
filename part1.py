"""
Part 1: MapReduce

In our first part, we will practice using MapReduce
to create several pipelines.
This part has 20 questions.

As you complete your code, you can run the code with

    python3 part1.py
    pytest part1.py

and you can view the output so far in:

    output/part1-answers.txt

In general, follow the same guidelines as in HW1!
Make sure that the output in part1-answers.txt looks correct.
See "Grading notes" here:
https://github.com/DavisPL-Teaching/119-hw1/blob/main/part1.py

For Q5-Q7, make sure your answer uses general_map and general_reduce as much as possible.
You will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

If you aren't sure of the type of the output, please post a question on Piazza.
"""

# Spark boilerplate (remember to always add this at the top of any Spark file)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

# Additional imports
import pytest

"""
===== Questions 1-3: Generalized Map and Reduce =====

We will first implement the generalized version of MapReduce.
It works on (key, value) pairs:

- During the map stage, for each (key1, value1) pairs we
  create a list of (key2, value2) pairs.
  All of the values are output as the result of the map stage.

- During the reduce stage, we will apply a reduce_by_key
  function (value2, value2) -> value2
  that describes how to combine two values.
  The values (key2, value2) will be grouped
  by key, then values of each key key2
  will be combined (in some order) until there
  are no values of that key left. It should end up with a single
  (key2, value2) pair for each key.

1. Fill in the general_map function
using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q1() answer. It should fill out automatically.
"""

def general_map(rdd, f):
    """
    rdd: an RDD with values of type (k1, v1)
    f: a function (k1, v1) -> List[(k2, v2)]
    output: an RDD with values of type (k2, v2)
    """
    return rdd.flatMap(lambda x: f(x[0], x[1]))

def test_general_map():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Map returning no values
    rdd2 = general_map(rdd1, lambda k, v: [])

    # Map returning length
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = rdd3.map(lambda pair: pair[1])

    # Map returnning odd or even length
    rdd5 = general_map(rdd1, lambda k, v: [(len(v) % 2, ())])

    assert rdd2.collect() == []
    assert sum(rdd4.collect()) == 14
    assert set(rdd5.collect()) == set([(1, ())])

def q1():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_map(rdd1, lambda k, v: [(1, v[-1])])
    return sorted(rdd2.collect())

"""
2. Fill in the reduce function using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q2() answer. It should fill out automatically.
"""

def general_reduce(rdd, f):
    """
    rdd: an RDD with values of type (k2, v2)
    f: a function (v2, v2) -> v2
    output: an RDD with values of type (k2, v2),
        and just one single value per key
    """
    return rdd.reduceByKey(f)

def test_general_reduce():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Reduce, concatenating strings of the same key
    rdd2 = general_reduce(rdd1, lambda x, y: x + y)
    res2 = set(rdd2.collect())

    # Reduce, adding lengths
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = general_reduce(rdd3, lambda x, y: x + y)
    res4 = sorted(rdd4.collect())

    assert (
        res2 == set([('c', "catcow"), ('d', "dog"), ('z', "zebra")])
        or res2 == set([('c', "cowcat"), ('d', "dog"), ('z', "zebra")])
    )
    assert res4 == [('c', 6), ('d', 3), ('z', 5)]

def q2():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_reduce(rdd1, lambda x, y: "hello")
    return sorted(rdd2.collect())

"""
3. Name one scenario where having the keys for Map
and keys for Reduce be different might be useful.

=== ANSWER Q3 BELOW ===

Map and Reduce can have different keys in a multi-process pipeline where the outputs
of the Map step are the keys for the Reduce step. For example, if we are computing sales,
the Map step can have (customer_id, transaction) as keys to produce outputs (product_category, price),
and the Reduce step can have (product_category) as a key and output the total sales per customer.
This way, the Map and Reduce steps have different keys, but constitute the same pipeline.

=== END OF Q3 ANSWER ===
"""

"""
===== Questions 4-10: MapReduce Pipelines =====

Now that we have our generalized MapReduce function,
let's do a few exercises.
For the first set of exercises, we will use a simple dataset that is the
set of integers between 1 and 1 million (inclusive).

4. First, we need a function that loads the input.
"""

def load_input():
    # Return a parallelized RDD with the integers between 1 and 1,000,000
    # This will be referred to in the following questions.
    return sc.parallelize(range(1, 1000001))

def q4(rdd):
    # Input: the RDD from load_input
    # Output: the length of the dataset.
    # You may use general_map or general_reduce here if you like (but you don't have to) to get the total count.
    return rdd.count()

"""
Now use the general_map and general_reduce functions to answer the following questions.

For Q5-Q7, your answers should use general_map and general_reduce as much as possible (wherever possible): you will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

5. Among the numbers from 1 to 1 million, what is the average value?
"""

def q5(rdd):
    # Input: the RDD from Q4
    # Output: the average value
    
    # all keys are the same
    all_numbers = rdd.map(lambda x: ('all', x))

    # Map: uses count as key (count=1 because each number is counted once), sum as value
    Map = general_map(all_numbers, lambda k, v: [('stats', (1, v))])

    # Reduce: combines counts and sums
    Reduce = general_reduce(Map, lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # obtain counts, sums --> calculate average
    count, sum = Reduce.collect()[0][1]
    return sum/count

"""
6. Among the numbers from 1 to 1 million, when written out,
which digit is most common, with what frequency?
And which is the least common, with what frequency?

(If there are ties, you may answer any of the tied digits.)

The digit should be either an integer 0-9 or a character '0'-'9'.
Frequency is the number of occurences of each value.

Your answer should use the general_map and general_reduce functions as much as possible.
"""

def q6(rdd):
    # Input: the RDD from Q4
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)

    # converts each digit to a string
    string_digits = rdd.map(lambda x: (None, str(x)))
    
    # Map: breaks a number into a string of digits, each with count 1
    Map = general_map(string_digits, lambda k, v: [(digit, 1) for digit in v])

    # Reduce: sum counts for each digit
    Reduce = general_reduce(Map, lambda x, y: x + y)

    # choose most and least common digits, and how many times they appear
    digit_counts = Reduce.collect()
    most_common = max(digit_counts, key=lambda x: x[1])
    least_common = min(digit_counts, key=lambda x: x[1])
    
    return (most_common[0], most_common[1], least_common[0], least_common[1])

"""
7. Among the numbers from 1 to 1 million, written out in English, which letter is most common?
With what frequency?
The least common?
With what frequency?

(If there are ties, you may answer any of the tied characters.)

For this part, you will need a helper function that computes
the English name for a number.

Please implement this without using an external library!
You should write this from scratch in Python.

Examples:

    0 = zero
    71 = seventy one
    513 = five hundred and thirteen
    801 = eight hundred and one
    999 = nine hundred and ninety nine
    1001 = one thousand one
    500,501 = five hundred thousand five hundred and one
    555,555 = five hundred and fifty five thousand five hundred and fifty five
    1,000,000 = one million

Notes:
- For "least frequent", count only letters which occur,
  not letters which don't occur.
- Please ignore spaces and hyphens.
- Use all lowercase letters.
- The word "and" should only appear after the "hundred" part, and nowhere else.
  It should appear after the hundreds if there are tens or ones in the same block.
  (Note the 1001 case above which differs from some other implementations!)
"""

# *** Define helper function(s) here ***
def number_as_words(n):
    # individual special cases (0 and 1 million)
    if n == 0:
        return "zero"
    if n == 1000000:
        return "one million"
    if n == 100000000:
        return "one hundred million"
    
    words = []

    # map numbers to words (need empty strings to account for index 0)
    ones = ["", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"]
    teens = ["ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"]
    tens = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"]

    # millions, ten millions, and hundred millions places
    if n >= 1000000:
        millions = n // 1000000
        if millions >= 10 and millions < 20:
            # 10,000,000-19,000,000
            words.append(teens[millions - 10])
        else:
            if millions >= 20:
                # 20,000,000-90,000,000
                words.append(tens[millions // 10])
            if millions % 10 > 0:
                # 1,000,000-9,000,000
                words.append(ones[millions % 10])
        words.append("million")
        # continue with remainder
        n = n % 1000000

    # thousands, ten thousands, and hundred thousands places
    if n >= 1000:
        thousands = n // 1000
        if thousands >= 100:
            # 100,000-900,000
            words.append(ones[thousands // 100] + " hundred")
            if thousands % 100 > 0:
                words.append("and")
        if thousands % 100 >= 10 and thousands % 100 < 20:
            # 10,000-19,000
            words.append(teens[thousands % 100 - 10])
        else:
            # 20,000-99,000
            if thousands % 100 >= 20:
                words.append(tens[(thousands % 100) // 10])
            # 1,000-9,000
            if thousands % 10 > 0:
                words.append(ones[thousands % 10])
        words.append("thousand")

    # hundreds place
    hundreds = n % 1000
    if hundreds >= 100:
        # 100-900
        words.append(ones[hundreds // 100] + " hundred")
        if hundreds % 100 > 0:
            words.append("and")
    
    # tens and ones places
    tens_and_ones = hundreds % 100
    # 10-19
    if tens_and_ones >= 10 and tens_and_ones < 20:
        words.append(teens[tens_and_ones - 10])
    else:
        # 20-99
        if tens_and_ones >= 20:
            words.append(tens[tens_and_ones // 10])
        # 1-9
        if (tens_and_ones % 10) > 0:
            words.append(ones[tens_and_ones % 10])

    return " ".join(words)

def q7(rdd):
    # Input: the RDD from Q4
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    
    # convert all numbers to words (excluding spaces)
    words = rdd.map(lambda x: (1, x))

    # Map: set each letter as the key, count=1 as its value
    Map = general_map(words, lambda k, v: [(letter, 1) for letter in number_as_words(v).replace(" ", "")])

    # Reduce: sum counts for each letter
    Reduce = general_reduce(Map, lambda x, y: x + y)
    

    # find most and least common letters and their counts
    letter_counts = Reduce.collect()
    most_common = max(letter_counts, key=lambda x: x[1])
    least_common = min(letter_counts, key=lambda x: x[1])
    
    return (most_common[0], most_common[1], least_common[0], least_common[1])

"""
8. Does the answer change if we have the numbers from 1 to 100,000,000?

Make a version of both pipelines from Q6 and Q7 for this case.
You will need a new load_input function.

Notes:
- The functions q8_a and q8_b don't have input parameters; they should call
  load_input_bigger directly.
- Please ensure that each of q8a and q8b runs in at most 3 minutes.
- If you are unable to run up to 100 million on your machine within the time
  limit, please change the input to 10 million instead of 100 million.
  If it is still taking too long even for that,
  you may need to change the number of partitions.
  For example, one student found that setting number of partitions to 100
  helped speed it up.
"""

def load_input_bigger():
    # only running 10 million due to time constraints
    return sc.parallelize(range(1, 10000001), 100)

def q8_a():
    # version of Q6
    # It should call into q6() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    rdd = load_input_bigger()
    return q6(rdd)

def q8_b():
    # version of Q7
    # It should call into q7() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    rdd = load_input_bigger()
    return q7(rdd)

"""
Discussion questions

9. State what types you used for k1, v1, k2, and v2 for your Q6 and Q7 pipelines.

=== ANSWER Q9 BELOW ===

For Q6, I used NoneType as k1 (placeholder key), String as v1 (number as string), String as k2 (individual digit as string), and Integer as v2 (count of 1 for each individual digit).
For Q7, I used Integer as k1 (placeholder key), Integer as v1 (original number), String as k2 (individual letter), and Integer as v2 (count of 1 for each individual letter).

=== END OF Q9 ANSWER ===

10. Do you think it would be possible to compute the above using only the
"simplified" MapReduce we saw in class? Why or why not?

=== ANSWER Q10 BELOW ===

Yes, it would be possible to do the same computation with the in-class MapReduce method, but it
would involve fixed key datatypes. We would have to encode them as strings to have a fixed datatype
for all keys, which would be less efficient than just having Map and Reduce in separate steps with
different key types like we have here.

=== END OF Q10 ANSWER ===
"""

"""
===== Questions 11-18: MapReduce Edge Cases =====

For the remaining questions, we will explore two interesting edge cases in MapReduce.

11. One edge case occurs when there is no output for the reduce stage.
This can happen if the map stage returns an empty list (for all keys).

Demonstrate this edge case by creating a specific pipeline which uses
our data set from Q4. It should use the general_map and general_reduce functions.

For Q11, Q14, and Q16:
your answer should return a Python set of (key, value) pairs after the reduce stage.
"""

def q11(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    
    # use dataset from q4, which only returns total size of dataset
    q4_data = rdd.map(lambda x: (1, x))

    # Map: returns empty list for all keys
    Map = general_map(q4_data, lambda k, v: [])

    # Reduce: no output
    Reduce = general_reduce(Map, lambda x, y: x + y)

    return set(Reduce.collect())

"""
12. What happened? Explain below.
Does this depend on anything specific about how
we chose to define general_reduce?

=== ANSWER Q12 BELOW ===

The output is an empty set, because the map function returns an empty list
for all key-value pairs. The reduce function never outputs anything if it has
no key-value pairs as input to be reduced.

=== END OF Q12 ANSWER ===

13. Lastly, we will explore a second edge case, where the reduce stage can
output different values depending on the order of the input.
This leads to something called "nondeterminism", where the output of the
pipeline can even change between runs!

First, take a look at the definition of your general_reduce function.
Why do you imagine it could be the case that the output of the reduce stage
is different depending on the order of the input?

=== ANSWER Q13 BELOW ===

If the reduce function is not associative, the partitioning or grouping of data
affects the results, leading to nondeterminism. For example, subtraction of multiple
numbers can have different results depending on grouping: a - (b - c) != (a - b) - c.

=== END OF Q13 ANSWER ===

14.
Now demonstrate this edge case concretely by writing a specific example below.
As before, you should use the same dataset from Q4.

Important: Please create an example where the output of the reduce stage is a set of (integer, integer) pairs.
(So k2 and v2 are both integers.)
"""

def q14(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs

    # change partition size to ensure nondeterminism
    rdd = rdd.repartition(100)

    # use data from q4, but use x % 10 as key to create multiple groupings of numbers for testing (based on ones place digit)
    q4_data = rdd.map(lambda x: (x % 10, x))
    
    # Map: do not change key-value pairs
    Map = general_map(q4_data, lambda k, v: [(k, v)])
    
    # Reduce: subtraction is nondeterministic, because the grouping of numbers can lead to different results
    Reduce = general_reduce(Map, lambda x, y: x - y)
    
    return set(Reduce.collect())

"""
15.
Run your pipeline. What happens?
Does it exhibit nondeterministic behavior on different runs?
(It may or may not! This depends on the Spark scheduler and implementation,
including partitioning.

=== ANSWER Q15 BELOW ===

We see that on different runs, the values that come from subtraction are different,
and they are always different for different digits (keys). This shows that subtraction
is a nondeterministic function if it is partitioned properly.

=== END OF Q15 ANSWER ===

16.
Lastly, try the same pipeline as in Q14
with at least 3 different levels of parallelism.

Write three functions, a, b, and c that use different levels of parallelism.
"""

def q16_a():
    # For this one, create the RDD yourself. Choose the number of partitions.
    rdd = sc.parallelize(range(1, 1000001), 10)
    q4_data = rdd.map(lambda x: (x % 10, x))
    Map = general_map(q4_data, lambda k, v: [(k, v)])
    Reduce = general_reduce(Map, lambda x, y: x - y)
    return set(Reduce.collect())

def q16_b():
    # For this one, create the RDD yourself. Choose the number of partitions.
    rdd = sc.parallelize(range(1, 1000001), 20)
    q4_data = rdd.map(lambda x: (x % 10, x))
    Map = general_map(q4_data, lambda k, v: [(k, v)])
    Reduce = general_reduce(Map, lambda x, y: x - y)
    return set(Reduce.collect())

def q16_c():
    # For this one, create the RDD yourself. Choose the number of partitions.
    rdd = sc.parallelize(range(1, 1000001), 50)
    q4_data = rdd.map(lambda x: (x % 10, x))
    Map = general_map(q4_data, lambda k, v: [(k, v)])
    Reduce = general_reduce(Map, lambda x, y: x - y)
    return set(Reduce.collect())

"""
Discussion questions

17. Was the answer different for the different levels of parallelism?

=== ANSWER Q17 BELOW ===

Yes, the answers were different for different numbers of partitions, because more partitions
means that there are more possible combinations of numbers, which makes it easier to have different
results of subtraction. With fewer partitions, there are fewer combinations, so the results will be
closer together (more deterministic).

=== END OF Q17 ANSWER ===

18. Do you think this would be a serious problem if this occured on a real-world pipeline?
Explain why or why not.

=== ANSWER Q18 BELOW ===

Yes, because nondeterministic results can make the process of debugging more difficult, due to the
different possible results, and can allow us to draw incorrect conclusions about data from our code.
This will ultimately lead us to have the wrong analysis of a problem, which would be misleading when
we present our findings.

=== END OF Q18 ANSWER ===

===== Q19-20: Further reading =====

19.
The following is a very nice paper
which explores this in more detail in the context of real-world MapReduce jobs.
https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/icsecomp14seip-seipid15-p.pdf

Take a look at the paper. What is one sentence you found interesting?

=== ANSWER Q19 BELOW ===

One sentence I found interesting was:

Flagging non-commutative reducers as bugs, as proposed in [3], is then likely to create many false positives that will frustrate programmers.

I believe that in the future, this might confuse me when I am trying to program, because sometimes I may need non-commutative
reduce functions, and I may be stuck if I find out that it produces nondeterministic results. But perhaps I will eventually
learn the methods to overcome them.

=== END OF Q19 ANSWER ===

20.
Take one example from the paper, and try to implement it using our
general_map and general_reduce functions.
For this part, just return the answer "True" at the end if you found
it possible to implement the example, and "False" if it was not.
"""

def q20():
    # I chose to use the example from Figure 2 in the paper
    example_data = [("a", 5), ("a", 3), ("b", 2), ("a", 1), ("b", 4)]
    rdd = sc.parallelize(example_data)

    try:
        test_data = rdd.map(lambda x: (x[0], x[1]))

        # Map: keep all key-value pairs
        Map = general_map(test_data, lambda k, v: [(k, v)])

        # Reduce: take sum of counts for each group
        Reduce = general_reduce(Map, lambda x, y: x + y)

        result = set(Reduce.collect())
        return result == {('a', 9), ('b', 6)}

    # in case implementing the example is impossible
    except Exception as e:
        return False

"""
That's it for Part 1!

===== Wrapping things up =====

**Don't modify this part.**

To wrap things up, we have collected
everything together in a pipeline for you below.

Check out the output in output/part1-answers.txt.
"""

ANSWER_FILE = "output/part1-answers.txt"
UNFINISHED = 0

def log_answer(name, func, *args):
    try:
        answer = func(*args)
        print(f"{name} answer: {answer}")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},{answer}\n')
            print(f"Answer saved to {ANSWER_FILE}")
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Not Implemented\n')
        global UNFINISHED
        UNFINISHED += 1

def PART_1_PIPELINE():
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = load_input()
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([])

    # Questions 1-3
    log_answer("q1", q1)
    log_answer("q2", q2)
    # 3: commentary

    # Questions 4-10
    log_answer("q4", q4, dfs)
    log_answer("q5", q5, dfs)
    log_answer("q6", q6, dfs)
    log_answer("q7", q7, dfs)
    log_answer("q8a", q8_a)
    log_answer("q8b", q8_b)
    # 9: commentary
    # 10: commentary

    # Questions 11-18
    log_answer("q11", q11, dfs)
    # 12: commentary
    # 13: commentary
    log_answer("q14", q14, dfs)
    # 15: commentary
    log_answer("q16a", q16_a)
    log_answer("q16b", q16_b)
    log_answer("q16c", q16_c)
    # 17: commentary
    # 18: commentary

    # Questions 19-20
    # 19: commentary
    log_answer("q20", q20)

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return f"{UNFINISHED} unfinished questions"

if __name__ == '__main__':
    log_answer("PART 1", PART_1_PIPELINE)

"""
=== END OF PART 1 ===
"""
