# Adding GPU Operator Support - Complete Guide for Contributors

This guide provides a comprehensive, step-by-step walkthrough for adding GPU-accelerated operator support to the RAPIDS Accelerator for Apache Spark. It includes a complete end-to-end example to help you understand the full implementation process.

## Table of Contents
- [SQL Operators 101 - Essential Background](#sql-operators-101---essential-background)
- [Prerequisites](#prerequisites)
- [Understanding the Architecture](#understanding-the-architecture)
- [Implementation Process Overview](#implementation-process-overview)
- [End-to-End Example: Adding GpuAcos](#end-to-end-example-adding-gpuacos)
- [Complex Operators - Beyond Simple Expressions](#complex-operators---beyond-simple-expressions)
- [Testing Strategy](#testing-strategy)
- [Build and Verification](#build-and-verification)
- [Common Pitfalls and Best Practices](#common-pitfalls-and-best-practices)
- [Next Steps](#next-steps)

## SQL Operators 101 - Essential Background

**New to SQL operators or Spark internals?** This section provides the foundational knowledge you need. If you're already familiar with Spark's internals, feel free to skip to [Prerequisites](#prerequisites).

### What is a SQL Operator?

In simple terms, a **SQL operator** is any operation that processes data in a SQL query. Think of operators as the building blocks of data processing.

**Examples of operators:**
- **Mathematical**: `acos(angle)`, `sqrt(number)`, `abs(value)`
- **Arithmetic**: `price + tax`, `quantity * price`
- **String**: `concat(first_name, last_name)`, `upper(city)`
- **Comparison**: `age > 18`, `status = 'active'`
- **Aggregation**: `sum(sales)`, `count(*)`, `avg(score)`
- **Joins**: Combining tables based on matching columns

### How Spark Processes SQL Queries

When you write a SQL query in Spark, it goes through several stages before execution:

```
Your SQL Query
     ↓
┌────────────────────┐
│  1. Parse          │  Convert SQL text to a tree structure
│  "SELECT sqrt(x)"  │
└────────┬───────────┘
         ↓
┌────────────────────┐
│  2. Analyze        │  Resolve column names, check types
│  Logical Plan      │
└────────┬───────────┘
         ↓
┌────────────────────┐
│  3. Optimize       │  Apply optimizations (Catalyst optimizer)
│  Optimized Plan    │
└────────┬───────────┘
         ↓
┌────────────────────┐
│  4. Physical Plan  │  Choose execution strategy ← WE INTERCEPT HERE!
│  CPU Operators     │
└────────┬───────────┘
         ↓
┌────────────────────┐
│  RAPIDS Plugin     │  Replace CPU → GPU operators
│  GPU Operators     │
└────────┬───────────┘
         ↓
     Execution
```

**This is where our plugin works:** We intercept at step 4 and replace CPU operators with GPU equivalents.

### Real-World Analogy

Think of processing data like a factory assembly line:

**CPU Processing (Traditional Spark):**
- One worker processes items sequentially
- Fast for small batches
- Slow for millions of items

**GPU Processing (RAPIDS Accelerator):**
- Thousands of workers process items in parallel
- Overhead to move items to the GPU assembly line
- Extremely fast for large batches (10-100x speedup!)

**Our job:** Replace the "CPU worker" (operator) with "GPU workers" (operator) in the assembly line.

### Expressions vs Physical Plans

Understanding the difference is crucial:

#### Expressions
**What they are:** Operations on data values (like functions in programming)

**Examples:**
```scala
// Mathematical expression
acos(angle)           // Calculate arc cosine

// Complex expression
sqrt(x * x + y * y)   // Calculate distance

// String expression
upper(name)           // Convert to uppercase
```

**In Spark's code:**
- Located in: `org.apache.spark.sql.catalyst.expressions`
- Base class: `Expression`
- GPU equivalent base: `GpuExpression`

#### Physical Plans
**What they are:** How data is processed and moved (like algorithms in programming)

**Examples:**
```scala
// Hash-based join algorithm
HashJoin(left_table, right_table, on = id)

// Sort operation
Sort(by = age, descending = true)

// Table scan
Scan(parquet_file)
```

**In Spark's code:**
- Located in: `org.apache.spark.sql.execution`
- Base class: `SparkPlan`
- GPU equivalent base: `GpuExec`

### Types of Operators You'll Work With

#### 1. **Unary Expressions** (One Input → One Output)
```sql
SELECT acos(angle) FROM table
       ^^^^
       Unary operator: takes one input (angle), returns one output
```

**Real-world example:**
```scala
// Input:  [0.5, 0.866, 1.0]
// acos()
// Output: [1.047, 0.523, 0.0]  (in radians)
```

#### 2. **Binary Expressions** (Two Inputs → One Output)
```sql
SELECT price + tax FROM table
       ^^^^^^^^^^^
       Binary operator: takes two inputs (price, tax), returns one output
```

**Real-world example:**
```scala
// Input A: [100, 200, 300]
// Input B: [10,  20,  30]
// add()
// Output:  [110, 220, 330]
```

#### 3. **Aggregate Expressions** (Many Inputs → One Output)
```sql
SELECT SUM(sales) FROM table
       ^^^^^^^^^^
       Aggregate: combines many values into one
```

**Real-world example:**
```scala
// Input:  [100, 200, 300, 400]
// sum()
// Output: 1000
```

#### 4. **Physical Operators** (Execution Strategies)
```sql
SELECT * FROM users JOIN orders ON users.id = orders.user_id
                     ^^^^
                     Physical operator: decides HOW to join (hash, sort, broadcast)
```

### Why GPU Acceleration Matters

**The Data Scale Problem:**
- Modern datasets: Millions to billions of rows
- Traditional CPU: Processes ~10,000-100,000 rows/second per core
- GPU: Processes ~1,000,000-10,000,000 rows/second

**When GPU helps most:**
- ✅ Large datasets (GB to TB)
- ✅ Columnar operations (math on entire columns)
- ✅ Parallel-friendly operations (no dependencies between rows)

**When CPU might be better:**
- ❌ Small datasets (< 100MB)
- ❌ Operations requiring sequential processing
- ❌ Complex string parsing with many conditionals

### Key Concepts for This Guide

#### Columnar Processing
Instead of processing rows one at a time, we process entire columns:

```
Row-based (CPU):                Columnar (GPU):
process row 1                   process all 1M values
process row 2                   of column 'price'
process row 3                   in parallel
...
process row 1,000,000          Result: 10-100x faster!
```

#### cuDF Library
- **What**: GPU DataFrame library (like pandas, but on GPU)
- **Why**: Provides optimized GPU operations
- **Where**: We call cuDF functions in our operator implementations

**Example:**
```scala
// CPU pandas-like:           // GPU cuDF:
column + 5                    column.add(Scalar.fromInt(5))
```

#### Type Signatures
Defines what data types an operator supports:

```scala
// This operator supports:
TypeSig.INT + TypeSig.LONG + TypeSig.DOUBLE
// Meaning: integers, long integers, and doubles

// This operator does NOT support:
TypeSig.STRING  // strings
TypeSig.ARRAY   // arrays
```

### What You'll Be Doing

When you add a GPU operator, you're essentially:

1. **Writing a GPU version** of a CPU operation
2. **Telling Spark** when it's safe to use the GPU version
3. **Testing** that GPU results match CPU results

**Simple example flow:**
```
User writes: SELECT acos(angle) FROM table

Spark creates: Acos(column_angle)  ← CPU version

Our plugin:    "I can do this on GPU!"
               Acos → GpuAcos       ← Replace

GPU executes:  cuDF.arccos(column) ← Fast!

Result:        Same answer, 10x faster
```

### Don't Worry If...

- **"I don't know Spark internals"** → This guide shows you exactly where to look
- **"I don't know GPU programming"** → cuDF handles the GPU code; you just call it
- **"I don't know Scala well"** → We provide copy-paste examples; just modify them
- **"I don't understand the math"** → cuDF implements the algorithms; you connect pieces

**You mainly need:**
- ✅ Ability to read and modify Scala code
- ✅ Understanding of what an operation does (e.g., "acos calculates arc cosine")
- ✅ Willingness to follow patterns in existing code

Ready? Let's dive in! 👇

## Prerequisites

### Knowledge Requirements

**Essential (must have):**
- ✅ Basic programming skills (any language)
- ✅ Ability to read and write Scala code (or willingness to learn)
- ✅ Understanding of basic SQL concepts
- ✅ Familiarity with command line and git

**Helpful (nice to have, but not required):**
- 💡 Apache Spark experience (we explain the internals as needed)
- 💡 GPU programming concepts (cuDF does the heavy lifting)
- 💡 Build tools experience (Maven, sbt)
- 💡 Python for integration testing

**Don't need to be an expert!** Most contributors learn Spark internals and GPU concepts while working on their first operator. The key is following existing patterns in the codebase.

### Development Environment
1. **Build tools**: Maven 3.6.0+ (tested with 3.6.x, 3.8.x, 3.9.x)
2. **JDK**: Java 8, 11, or 17 (JDK 8 is the primary version)
3. **Scala**: 2.12 or 2.13
4. **IDE**: IntelliJ IDEA, VS Code with Metals, or similar (see [CONTRIBUTING.md](../../CONTRIBUTING.md#setting-up-an-integrated-development-environment))
5. **Git submodules**: Run `git submodule update --init` after cloning

### Initial Setup
```bash
# Clone the repository
git clone https://github.com/NVIDIA/spark-rapids.git
cd spark-rapids

# Update submodules
git submodule update --init

# Build for a specific Spark version (e.g., Spark 3.3.0)
mvn -Dbuildver=330 verify
```

## Understanding the Architecture

### Plugin Architecture Overview

The RAPIDS Accelerator uses a **plugin architecture** that intercepts Spark's physical execution plan and replaces CPU operators with GPU equivalents. The key components are:

```
┌─────────────────────────────────────────────────────────────┐
│                    Spark Query Plan                         │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│          RapidsPluginQueryExtension                         │
│  (Intercepts and transforms physical plan)                  │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                  GpuOverrides.scala                         │
│  • Defines rules for replacing CPU → GPU operators          │
│  • Type signature validation                                │
│  • Operator registration                                    │
└─────────────────────┬───────────────────────────────────────┘
                      │
         ┌────────────┴────────────┐
         ▼                         ▼
┌──────────────────┐      ┌──────────────────┐
│   Meta Layer     │      │  GPU Operators   │
│ (RapidsMeta)     │      │  (GpuExpression) │
│ • Validation     │      │  • Execution     │
│ • Tagging        │      │  • cuDF calls    │
└──────────────────┘      └──────────────────┘
```

### Key Components

#### 1. **GpuOverrides.scala**
The central registry where all operator transformation rules are defined. Located at:
```
sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuOverrides.scala
```

#### 2. **RapidsMeta Classes**
Metadata wrappers that analyze operator compatibility before transformation:
- `UnaryExprMeta[T]` - For unary expressions (e.g., `Acos`, `Abs`)
- `BinaryExprMeta[T]` - For binary expressions (e.g., `Add`, `Subtract`)
- `AggExprMeta[T]` - For aggregate expressions (e.g., `Sum`, `Count`)
- `SparkPlanMeta[T]` - For physical execution plan nodes

#### 3. **GPU Expression Implementations**
Actual GPU operator implementations that execute on the GPU:
```
sql-plugin/src/main/scala/org/apache/spark/sql/rapids/
├── mathExpressions.scala      # Math operations (sin, cos, acos, etc.)
├── arithmetic.scala           # Arithmetic operations (+, -, *, /)
├── stringExpressions.scala    # String operations
├── datetimeExpressions.scala  # Date/time operations
└── execution/                 # Physical plan operators
```

#### 4. **Type Checking System**
The plugin uses a sophisticated type signature system to validate supported data types:
- `TypeSig` - Defines supported types (e.g., `INT`, `LONG`, `DOUBLE`, `STRING`)
- `ExprChecks` - Pre-defined type check patterns (e.g., `mathUnary`, `binaryProject`)

### Data Flow

```
Input Data (CPU)
      ↓
ColumnarBatch (GPU)
      ↓
GpuColumnVector (cuDF Column)
      ↓
GPU Operator (cuDF operations)
      ↓
Result ColumnVector
      ↓
ColumnarBatch (Output)
```

## Implementation Process Overview

Adding a new operator involves **five main phases**:

### Phase 1: Research and Planning
1. Identify the Spark operator to accelerate
2. Find similar existing GPU operators in the codebase
3. Verify cuDF support for the required operation
4. Define supported data types

### Phase 2: Implement GPU Operator
1. Create GPU operator class extending appropriate base class
2. Implement the columnar operation using cuDF
3. Handle edge cases and nulls

### Phase 3: Create Metadata Wrapper
1. Create a Meta class for validation and tagging
2. Implement `tagExprForGpu()` for compatibility checks
3. Implement `convertToGpu()` for CPU → GPU conversion

### Phase 4: Register in GpuOverrides
1. Add operator rule to `GpuOverrides.scala`
2. Define type signatures and checks
3. Link Meta class to operator

### Phase 5: Testing and Validation
1. Add unit tests (Scala)
2. Add integration tests (Python)
3. Verify GPU vs CPU result equivalence
4. Check scalastyle compliance

## End-to-End Example: Adding GpuAcos

Let's walk through adding GPU support for the `Acos` (arc cosine) operator. This example covers every step from implementation to testing.

### Step 1: Research Existing Patterns

First, examine similar operators to understand the patterns:

```bash
# Find existing math operators
grep -r "GpuAsin\|GpuCos\|GpuSin" sql-plugin/src/main/scala/org/apache/spark/sql/rapids/
```

**Key findings:**
- Math operators extend `CudfUnaryMathExpression` or `GpuUnaryMathExpression`
- cuDF provides `UnaryOp.ARCCOS` for arc cosine operations
- Input/output types are typically `DoubleType`

### Step 2: Implement GPU Operator

Create the GPU operator in `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/mathExpressions.scala`:

```scala
case class GpuAcos(child: Expression) extends CudfUnaryMathExpression("ACOS") {
  override def unaryOp: UnaryOp = UnaryOp.ARCCOS
  override def outputTypeOverride: DType = DType.FLOAT64
}
```

**Explanation:**
- `case class GpuAcos` - Defines the GPU operator
- `extends CudfUnaryMathExpression("ACOS")` - Inherits common math operator behavior
- `unaryOp: UnaryOp = UnaryOp.ARCCOS` - Maps to cuDF's arc cosine operation
- `outputTypeOverride: DType = DType.FLOAT64` - Specifies output as double precision

**Base Class Hierarchy:**
```
GpuUnaryMathExpression("ACOS")
      ↑
CudfUnaryMathExpression("ACOS")
      ↑
GpuAcos
```

### Step 3: Create Metadata Wrapper

In `sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuOverrides.scala`, create a Meta class:

```scala
expr[Acos](
  "Inverse cosine",
  ExprChecks.mathUnaryWithAst,
  (a, conf, p, r) => new UnaryAstExprMeta[Acos](a, conf, p, r) {
    override def convertToGpu(child: Expression): GpuExpression = GpuAcos(child)
  })
```

**Explanation:**
- `expr[Acos]` - Registers a rule for Spark's `Acos` expression
- `"Inverse cosine"` - Human-readable description
- `ExprChecks.mathUnaryWithAst` - Pre-defined type checks for math unary operations
- `UnaryAstExprMeta[Acos]` - Metadata wrapper with AST (Abstract Syntax Tree) support
- `convertToGpu(child)` - Converts CPU expression to GPU expression

**What is ExprChecks.mathUnaryWithAst?**
This pre-defined check specifies:
- **Plugin supports**: `DOUBLE`, `FLOAT` (with GPU fallback for edge cases)
- **Spark supports**: All numeric types (Spark will cast inputs appropriately)
- Defined in `ExprChecks.scala` as:
```scala
val mathUnaryWithAst = ExprChecks.unaryProjectAndAst(
  TypeSig.DOUBLE,           // Plugin supported types
  TypeSig.DOUBLE,           // Spark supported types
  TypeSig.DOUBLE,           // AST supported types
  TypeSig.DOUBLE)           // AST Spark types
```

### Step 4: Register in GpuOverrides

The registration in Step 3 adds the operator to the `commonExpressions` map in `GpuOverrides.scala`. This map is located around line 1247:

```scala
// File: sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuOverrides.scala
val commonExpressions: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
  // ... other operators ...
  expr[Acos](
    "Inverse cosine",
    ExprChecks.mathUnaryWithAst,
    (a, conf, p, r) => new UnaryAstExprMeta[Acos](a, conf, p, r) {
      override def convertToGpu(child: Expression): GpuExpression = GpuAcos(child)
    }),
  // ... more operators ...
).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
```

**Registration happens automatically** when the plugin initializes and builds the operator map.

### Step 5: Verify Operator Implementation

Check that the implementation follows project patterns:

**Location references:**
- GPU Implementation: `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/mathExpressions.scala:43-46`
- Registration: `sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuOverrides.scala:1247-1252`

**Key verification points:**
1. ✅ Extends correct base class (`CudfUnaryMathExpression`)
2. ✅ Uses appropriate cuDF operation (`UnaryOp.ARCCOS`)
3. ✅ Specifies correct output type (`DType.FLOAT64`)
4. ✅ Registered in `GpuOverrides.scala`
5. ✅ Uses standard type checks (`ExprChecks.mathUnaryWithAst`)

## Complex Operators - Beyond Simple Expressions

Now that you've mastered simple unary operators like `GpuAcos`, you may want to tackle more complex operators. This section explains what makes operators complex and provides detailed examples for aggregate functions and joins.

### What Makes an Operator "Complex"?

Complex operators differ from simple expressions in several ways:

| Complexity Factor | Simple (Acos) | Complex (Join/Aggregate) |
|-------------------|---------------|--------------------------|
| **Number of inputs** | Single column | Multiple tables/columns |
| **State management** | Stateless | May maintain state/buffers |
| **Memory usage** | Minimal | Can cache/buffer data |
| **Base classes** | `UnaryExprMeta` | `SparkPlanMeta`, `AggExprMeta` |
| **Files to modify** | 2 files | 3-5 files |
| **Testing complexity** | Unit tests sufficient | Needs extensive integration tests |
| **Edge cases** | Few (nulls, NaN) | Many (empty tables, skew, OOM) |
| **Spark knowledge** | Basic | Moderate to advanced |

### Operator Complexity Spectrum

Understanding where an operator falls on the complexity spectrum helps you gauge the effort required:

```
Level 1: Simple Unary Expressions
├─ acos, sqrt, abs, upper, lower
├─ Single input → single output
├─ No state, pure functions
└─ Example: GpuAcos

Level 2: Binary Expressions
├─ add, multiply, concat, contains
├─ Two inputs → single output
├─ Still stateless
└─ Example: GpuAdd

Level 3: Aggregate Functions
├─ sum, count, avg, max, min
├─ Many inputs → single or grouped outputs
├─ Maintains aggregation state
└─ Example: GpuSum

Level 4: Window Functions
├─ rank, row_number, running_sum
├─ Operates on partitions with ordering
├─ Complex state management
└─ Example: GpuRunningSum

Level 5: Join Operations
├─ inner, left_outer, right_outer, anti, semi
├─ Combines two tables
├─ Build/stream sides, broadcast vs shuffle
└─ Example: GpuBroadcastHashJoin

Level 6: Multi-Stage Physical Plans
├─ Sort-merge-join, complex aggregates
├─ Multiple execution stages
├─ Advanced optimization and memory management
└─ Example: GpuSortMergeJoin
```

**Recommendation:** Start at Level 1, build 2-3 operators, then progress to Level 2, and so on.

### Example 1: Aggregate Function (GpuSum)

Aggregate functions are more complex than simple expressions because they maintain state across multiple input rows.

#### How Aggregates Work

```
Input Data:          Aggregate Process:           Output:
[1, 2, 3, 4, 5]  →  Initialize: sum = 0     →   15
                     Update: sum += 1
                     Update: sum += 2
                     Update: sum += 3
                     Update: sum += 4
                     Update: sum += 5
                     Finalize: return sum
```

In distributed scenarios, aggregates also need to **merge** partial results:

```
Worker 1: [1, 2, 3]  →  Partial: 6  ┐
Worker 2: [4, 5]     →  Partial: 9  ├→  Merge: 6 + 9 = 15
```

#### GPU Sum Implementation

**File:** `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/aggregate/aggregateFunctions.scala`

```scala
// Wrapper that delegates to cuDF's sum aggregation
class CudfSum(override val dataType: DataType) extends CudfAggregate {
  @transient lazy val rapidsSumType: DType =
    GpuColumnVector.getNonNestedRapidsType(dataType)

  // For full-column reduction (no grouping)
  override val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum(rapidsSumType)

  // For group-by aggregation
  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.sum()

  override val name: String = "CudfSum"
}

// High-level GPU Sum expression
abstract class GpuSum(
    child: Expression,
    resultType: DataType,
    failOnErrorOverride: Boolean)
    extends GpuAggregateFunction
    with ImplicitCastInputTypes
    with Serializable {

  // Initial value for aggregation state
  override lazy val initialValues: Seq[GpuLiteral] =
    Seq(GpuLiteral(0, dataType))

  // Project input to prepare for aggregation
  override lazy val inputProjection: Seq[Expression] = Seq(child)

  // Update aggregation state with new input
  override lazy val updateAggregates: Seq[CudfAggregate] =
    Seq(new CudfSum(dataType))

  // Merge partial aggregates from different partitions
  override lazy val mergeAggregates: Seq[CudfAggregate] =
    Seq(new CudfSum(dataType))

  // Final result expression
  override lazy val evaluateExpression: Expression = aggBufferAttributes.head

  override val dataType: DataType = resultType
  override def nullable: Boolean = true
  override def children: Seq[Expression] = Seq(child)
}
```

#### Key Concepts for Aggregates

1. **Initial Values**: Starting state for the aggregation (e.g., 0 for sum, empty list for collect)

2. **Input Projection**: How to transform input before aggregating

3. **Update Aggregates**: How to incorporate new rows into the aggregate state

4. **Merge Aggregates**: How to combine partial results from different workers

5. **Evaluate Expression**: How to compute the final result from the aggregate state

#### Registering an Aggregate

**File:** `sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuOverrides.scala` (around line 2322)

```scala
expr[Sum](
  "Sum aggregate operator",
  ExprChecks.fullAgg(
    // Output types
    TypeSig.LONG + TypeSig.DOUBLE + TypeSig.DECIMAL_128,
    TypeSig.LONG + TypeSig.DOUBLE + TypeSig.DECIMAL_128,
    // Input parameter checks
    Seq(ParamCheck("input", TypeSig.gpuNumeric, TypeSig.cpuNumeric))),
  (a, conf, p, r) => new AggExprMeta[Sum](a, conf, p, r) {
    override def tagAggForGpu(): Unit = {
      val inputDataType = a.child.dataType
      // Additional validation logic for decimal overflow, etc.
      GpuOverrides.checkAndTagFloatAgg(inputDataType, conf, this)
    }

    override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
      val child = childExprs.head
      GpuSum(child, a.dataType, failOnError = SQLConf.get.ansiEnabled)
    }
  })
```

**What's different from simple expressions:**
- Uses `AggExprMeta` instead of `UnaryExprMeta`
- Uses `ExprChecks.fullAgg()` for type checking
- Implements `tagAggForGpu()` for aggregate-specific validation
- More complex state management in the GPU implementation

#### Testing Aggregates

**Integration test example:**
```python
# File: integration_tests/src/main/python/hash_aggregate_test.py

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_hash_agg_with_sum(data_gen):
    """Test SUM aggregate with hash aggregation"""
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, [
            ('a', data_gen),
            ('b', IntegerGen())
        ]).groupBy('b').agg(f.sum('a')))
```

### Example 2: Join Operations (BroadcastHashJoin)

Joins are among the most complex operators because they:
- Process two input tables (left and right)
- Require choosing a build side (smaller table) and stream side
- Support multiple join types (inner, outer, semi, anti)
- Must handle broadcast data distribution
- Have significant memory implications

#### How Broadcast Hash Join Works

```
Left Table (Large)      Right Table (Small, Broadcast)
┌────┬───────┐          ┌────┬────────┐
│ ID │ Name  │          │ ID │ City   │
├────┼───────┤          ├────┼────────┤
│ 1  │ Alice │          │ 1  │ NYC    │  ← Broadcasted to all workers
│ 2  │ Bob   │          │ 3  │ LA     │
│ 3  │ Carol │          └────┴────────┘
└────┴───────┘
     ↓                        ↓
   Stream Side            Build Side (Hash Table)

Join Process:
1. Broadcast right table to all workers
2. Build hash table from right table (ID → City)
3. Stream through left table
4. For each left row, probe hash table with join key
5. Output matched rows

Result:
┌────┬───────┬──────┐
│ ID │ Name  │ City │
├────┼───────┼──────┤
│ 1  │ Alice │ NYC  │
│ 3  │ Carol │ LA   │
└────┴───────┴──────┘
```

#### Architecture Overview

```
BroadcastHashJoinExec (Spark CPU)
       ↓
GpuBroadcastHashJoinMeta (Validation & Tagging)
  │
  ├─ Check join type supported
  ├─ Validate join keys are GPU-compatible
  ├─ Verify build side can fit in GPU memory
  ├─ Check condition expression (if any)
  └─ Tag children for GPU replacement
       ↓
GpuBroadcastHashJoinExecBase (GPU Execution)
  │
  ├─ Receive broadcast hash table
  ├─ Stream through other side
  ├─ Probe hash table using cuDF join APIs
  └─ Return joined result batches
```

#### Meta Class (Validation Layer)

**File:** `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuBroadcastHashJoinExecBase.scala`

```scala
abstract class GpuBroadcastHashJoinMetaBase(
    join: BroadcastHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends GpuBroadcastJoinMeta[BroadcastHashJoinExec](join, conf, parent, rule) {

  // Wrap join keys for validation
  val leftKeys: Seq[BaseExprMeta[_]] =
    join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val rightKeys: Seq[BaseExprMeta[_]] =
    join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  // Wrap optional join condition
  val conditionMeta: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  // Determine which side to build (smaller table)
  val buildSide: GpuBuildSide = GpuJoinUtils.getGpuBuildSide(join.buildSide)

  override def tagPlanForGpu(): Unit = {
    // Validate join type and keys
    GpuHashJoin.tagJoin(this, join.joinType, buildSide,
      join.leftKeys, join.rightKeys, conditionMeta)

    // Validate build side compatibility
    GpuHashJoin.tagBuildSide(this, join.joinType, buildSide)

    // Check if broadcast side can be on GPU
    val buildSideMeta = buildSide match {
      case GpuBuildLeft => childPlans.head
      case GpuBuildRight => childPlans(1)
    }

    if (!canBuildSideBeReplaced(buildSideMeta)) {
      willNotWorkOnGpu("the broadcast for this join must be on the GPU too")
    }
  }

  def convertToGpu(): GpuExec  // Implemented by subclasses
}
```

**What the Meta class does:**
1. **Wraps join keys**: Validates each join key expression can run on GPU
2. **Determines build side**: Chooses which table to broadcast based on size
3. **Validates join type**: Checks if GPU supports this join type (Inner, LeftOuter, etc.)
4. **Checks broadcast compatibility**: Ensures broadcast data is also on GPU
5. **Tags for GPU**: Marks the join for GPU execution if all checks pass

#### GPU Execution Class

```scala
abstract class GpuBroadcastHashJoinExecBase(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
  extends ShimBinaryExecNode with GpuHashJoin {

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val builtTable = buildSide match {
      case GpuBuildLeft => left
      case GpuBuildRight => right
    }

    // Get broadcast hash table
    val broadcastRelation = builtTable.executeBroadcast[SerializeConcatHostBuffersDeserializeBatch]()

    val streamedPlan = buildSide match {
      case GpuBuildLeft => right
      case GpuBuildRight => left
    }

    // Stream through the other side and join with broadcast table
    streamedPlan.executeColumnar().mapPartitions { streamedIter =>
      val builtBatch = broadcastRelation.value.batch

      withResource(builtBatch) { built =>
        // Use cuDF hash join
        doJoin(built, streamedIter, buildSide, joinType, condition)
      }
    }
  }

  // Actual join logic using cuDF
  def doJoin(
      builtTable: ColumnarBatch,
      streamIter: Iterator[ColumnarBatch],
      targetSize: Long): Iterator[ColumnarBatch] = {

    // Convert to cuDF tables
    val builtCudfTable = GpuColumnVector.from(builtTable)

    streamIter.flatMap { streamBatch =>
      withResource(GpuColumnVector.from(streamBatch)) { streamTable =>
        // Perform cuDF hash join
        val joinResult = joinType match {
          case Inner => streamTable.innerJoin(builtCudfTable, leftKeys, rightKeys)
          case LeftOuter => streamTable.leftJoin(builtCudfTable, leftKeys, rightKeys)
          case RightOuter => streamTable.rightJoin(builtCudfTable, leftKeys, rightKeys)
          // ... other join types
        }

        // Convert result back to ColumnarBatch
        withResource(joinResult) { result =>
          GpuColumnVector.from(result, outputSchema)
        }
      }
    }
  }
}
```

#### Registering a Join

**File:** `sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuOverrides.scala` (around line 4493)

```scala
exec[BroadcastHashJoinExec](
  "Implementation of join using broadcast data",
  JoinTypeChecks.equiJoinExecChecks,  // Pre-defined checks for joins
  (join, conf, p, r) => new GpuBroadcastHashJoinMeta(join, conf, p, r))
```

**What's different from expressions:**
- Uses `exec[...]` instead of `expr[...]` (it's a physical plan operator)
- Uses `JoinTypeChecks.equiJoinExecChecks` for comprehensive join validation
- Meta class is much more complex with multiple validation steps
- GPU execution involves RDD operations and partition-level processing

#### Join Type Checks Explained

```scala
object JoinTypeChecks {
  // Supported data types for join keys
  private val cudfSupportedKeyTypes =
    (TypeSig.commonCudfTypes + TypeSig.NULL +
     TypeSig.DECIMAL_128 + TypeSig.STRUCT).nested()

  // Data types that can "ride along" in join (non-key columns)
  private val joinRideAlongTypes =
    (cudfSupportedKeyTypes + TypeSig.BINARY +
     TypeSig.ARRAY + TypeSig.MAP).nested()

  val equiJoinExecChecks: ExecChecks = ExecChecks(
    joinRideAlongTypes,  // What can be in output
    TypeSig.all,         // What Spark supports
    Map(
      LEFT_KEYS -> InputCheck(cudfSupportedKeyTypes, sparkSupportedJoinKeyTypes),
      RIGHT_KEYS -> InputCheck(cudfSupportedKeyTypes, sparkSupportedJoinKeyTypes),
      CONDITION -> InputCheck(TypeSig.BOOLEAN, TypeSig.BOOLEAN)))
}
```

**What this means:**
- **Left/Right Keys**: Must be types cuDF can hash and compare (no maps or complex arrays)
- **Ride-along columns**: Can be almost any type (including arrays, maps)
- **Condition**: Optional boolean expression for additional filtering

#### Testing Joins

**Integration test example:**
```python
# File: integration_tests/src/main/python/join_test.py

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('join_type', ['Inner', 'Left', 'Right', 'FullOuter'], ids=idfn)
def test_broadcast_hash_join(data_gen, join_type):
    """Test broadcast hash join with various data types and join types"""
    def do_join(spark):
        left_df = binary_op_df(spark, data_gen)
        right_df = binary_op_df(spark, data_gen)
        # Use broadcast hint to force broadcast join
        return left_df.join(broadcast(right_df),
                           left_df.a == right_df.a,
                           join_type)

    assert_gpu_and_cpu_are_equal_collect(do_join, conf={
        'spark.sql.autoBroadcastJoinThreshold': '10MB'
    })
```

### Key Differences: Simple vs. Complex Operators

| Aspect | Simple (GpuAcos) | Aggregate (GpuSum) | Join (BroadcastHashJoin) |
|--------|------------------|---------------------|--------------------------|
| **Inputs** | 1 column | Multiple rows | 2 tables |
| **State** | Stateless | Aggregation buffer | Hash table |
| **Base class** | `UnaryExprMeta` | `AggExprMeta` | `SparkPlanMeta` |
| **Registration** | `expr[Acos]` | `expr[Sum]` | `exec[BroadcastHashJoinExec]` |
| **Type checks** | `ExprChecks.mathUnary` | `ExprChecks.fullAgg` | `JoinTypeChecks.equiJoinExecChecks` |
| **Files modified** | 2 (impl + registry) | 3 (impl + registry + tests) | 4-5 (meta + exec + registry + tests) |
| **Validation** | Type compatibility | Type + overflow checks | Type + build side + memory + distribution |
| **cuDF API** | `column.arccos()` | `column.sum()` | `table.leftJoin(other)` |
| **Memory** | Input size | Aggregation buffer size | Build side must fit in memory |
| **Testing** | Unit + integration | Unit + integration + aggregate-specific | Unit + integration + join-specific |

### When to Tackle Complex Operators

#### ✅ You're Ready When:

1. **You've completed 2-3 simple operators successfully**
   - Comfortable with the build process
   - Understand type signatures
   - Know how to write and run tests

2. **You understand the operator's Spark implementation**
   - Read Spark's source code for the operator
   - Understand the algorithm and edge cases
   - Know why certain design choices were made

3. **cuDF supports the operation**
   - Check cuDF documentation
   - Verify the operation exists and is stable
   - Understand cuDF's API and limitations

4. **You have time for extensive testing**
   - Complex operators have many edge cases
   - Testing takes significantly longer
   - May require performance tuning

#### ⚠️ Consider Waiting If:

- **This is your first contribution** → Start with Level 1-2 operators
- **Limited Spark knowledge** → Study Spark internals first
- **Tight deadline** → Complex operators take weeks, not days
- **Unsure about cuDF support** → Verify cuDF capabilities first

### Practical Tips for Complex Operators

#### 1. **Start by Copying a Similar Operator**

```bash
# For aggregates, copy from existing aggregate
cp aggregateFunctions.scala my_new_aggregate.scala

# For joins, study existing join implementation
# Don't copy - joins are too complex and specific
```

#### 2. **Incremental Development**

```
Week 1: Meta class + basic validation
Week 2: GPU implementation for simplest case (e.g., Inner join only)
Week 3: Add support for other cases (LeftOuter, RightOuter, etc.)
Week 4: Testing and edge case handling
Week 5: Performance tuning and optimization
```

#### 3. **Use GPU Memory Profiling**

```scala
// Add logging to track memory usage
logInfo(s"Build side size: ${builtTable.sizeInBytes} bytes")
GpuSemaphore.acquireIfNecessary(TaskContext.get())
```

#### 4. **Test Incrementally**

```bash
# Start with tiny datasets
./runtests.py -k test_my_join --debug --limit_rows=100

# Gradually increase size
./runtests.py -k test_my_join --limit_rows=10000

# Finally, test at scale
./runtests.py -k test_my_join
```

#### 5. **Pay Attention to Build Side Validation**

For joins, incorrect build side selection can cause OOM:

```scala
override def tagPlanForGpu(): Unit = {
  // Always validate build side can fit in GPU memory
  val buildSideSize = estimateBuildSideSize()
  val gpuMemory = getAvailableGpuMemory()

  if (buildSideSize > gpuMemory * 0.8) {  // Leave 20% headroom
    willNotWorkOnGpu(s"Build side too large: ${buildSideSize} bytes")
  }
}
```

#### 6. **Understand Data Distribution**

Joins and aggregates are affected by data distribution:

```
Skewed Data:               Uniform Data:
Worker 1: 90% of keys     Worker 1: 25% of keys
Worker 2: 8% of keys      Worker 2: 25% of keys
Worker 3: 2% of keys      Worker 3: 25% of keys
                          Worker 4: 25% of keys
↓ May cause OOM           ↓ Balanced, works well
```

Test with both uniform and skewed data distributions.

### Next Steps After Mastering Complex Operators

Once comfortable with aggregates and joins:

1. **Window Functions** - Combines aspects of both
2. **Custom Aggregates** - UDAFs with complex logic
3. **Optimizing Existing Operators** - Performance tuning
4. **Contributing to cuDF** - Add missing operations

**Remember:** Complex doesn't mean impossible! Take your time, study existing code, test thoroughly, and don't hesitate to ask for help.

## Testing Strategy

The RAPIDS Accelerator uses a **multi-layer testing approach**:

### Layer 1: Unit Tests (Scala)

Unit tests extend Spark's native test suites to inherit test coverage:

```scala
// File: tests/src/test/spark330/scala/org/apache/spark/sql/rapids/suites/RapidsMathFunctionsSuite.scala
package org.apache.spark.sql.rapids.suites

import org.apache.spark.sql.MathFunctionsSuite
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsMathFunctionsSuite extends MathFunctionsSuite with RapidsSQLTestsTrait {
  // Automatically inherits all Spark's math function tests
  // Tests run with GPU plugin enabled via RapidsSQLTestsTrait
}
```

**What this does:**
- Extends Spark's `MathFunctionsSuite` to inherit all native tests
- Mixes in `RapidsSQLTestsTrait` to enable GPU execution
- Tests verify GPU results match CPU results

**Running unit tests:**
```bash
# Run all tests for Spark 3.3.0
mvn -Dbuildver=330 test

# Run specific test suite
mvn -Dbuildver=330 test -Dtest=RapidsMathFunctionsSuite
```

### Layer 2: Integration Tests (Python)

Integration tests use pytest to verify end-to-end functionality:

```python
# File: integration_tests/src/main/python/arithmetic_ops_test.py

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_acos(data_gen):
    """Test GPU acos matches CPU acos for various double inputs"""
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr('acos(a)'))
```

**Explanation:**
- `@approximate_float` - Allows small floating-point differences (1e-6 relative error)
- `@pytest.mark.parametrize` - Tests with multiple data generators
- `double_gens` - Various double data patterns (normal, special values, nulls, etc.)
- `assert_gpu_and_cpu_are_equal_collect` - Runs query on both CPU and GPU, compares results

**Running integration tests:**
```bash
cd integration_tests

# Run specific test
./runtests.py -k test_acos

# Run all arithmetic tests
./runtests.py arithmetic_ops_test.py

# Run with specific Spark version
./runtests.py --runtime_env="spark" --spark_ver=3.3.0 -k test_acos
```

### Layer 3: AST (Abstract Syntax Tree) Tests

For operations supporting AST optimization:

```python
# File: integration_tests/src/main/python/ast_test.py

@pytest.mark.parametrize('data_gen', [DoubleGen()], ids=idfn)
def test_acos_ast(data_gen):
    """Test acos in AST (fused) operations"""
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, data_gen).selectExpr(
            'acos(a) + acos(b)',  # Multiple acos in single expression
            'acos(a * 2.0)'))     # acos with nested operations
```

**AST Benefits:**
- Fuses multiple operations into single GPU kernel
- Reduces memory transfers between GPU operations
- Improves performance for complex expressions

### Testing Edge Cases

Always test these scenarios:

1. **Null handling**: `NULL` inputs should produce `NULL` outputs
2. **Special values**: `NaN`, `Infinity`, `-Infinity`
3. **Domain boundaries**: For `acos`, test values at `-1.0`, `1.0` (valid range)
4. **Out of domain**: Values `< -1` or `> 1` should return `NaN`
5. **Type compatibility**: Test with different numeric types

**Example comprehensive test:**
```python
@pytest.mark.parametrize('data_gen', [
    DoubleGen(),                           # Normal values
    DoubleGen(nullable=True),              # With nulls
    DoubleGen(special_cases=True),         # NaN, Inf, -Inf
    DoubleGen(min_val=-1.0, max_val=1.0),  # Valid acos domain
], ids=idfn)
def test_acos_comprehensive(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr('acos(a)'))
```

## Build and Verification

### Build Process

```bash
# Clean build for Spark 3.3.0
mvn clean verify -Dbuildver=330

# Skip tests for faster iteration
mvn clean install -Dbuildver=330 -DskipTests

# Build for multiple Spark versions (creates uber jar)
./build/buildall --profile=noSnapshots
```

**Important Build Options:**
- `-Dbuildver=XXX` - Target Spark version (320, 330, 340, etc.)
- `-DskipTests` - Skip tests (use for quick iterations)
- `-Drat.skip=true` - Skip license checks
- `-Dcuda.version=cuda12` - Target CUDA version (default: cuda12)

### Verify Scalastyle Compliance

The project enforces strict code style rules:

```bash
# Check scalastyle for all modules
mvn scalastyle:check

# Check specific module
mvn -pl sql-plugin scalastyle:check
```

**Common scalastyle violations:**
1. **Line length > 100 characters**
   ```scala
   // Bad
   case class GpuAcos(child: Expression) extends CudfUnaryMathExpression("ACOS") with SomeOtherTrait with YetAnotherTrait {

   // Good
   case class GpuAcos(child: Expression)
     extends CudfUnaryMathExpression("ACOS")
     with SomeOtherTrait
     with YetAnotherTrait {
   ```

2. **Missing newline at end of file**
   - Ensure your editor adds a final newline

3. **Trailing whitespace**
   ```bash
   # Fix all trailing whitespace
   find . -name "*.scala" -exec sed -i '' 's/[[:space:]]*$//' {} \;
   ```

4. **Using Class.forName directly**
   ```scala
   // Bad
   Class.forName("com.example.MyClass")

   // Good
   Utils.classForName("com.example.MyClass")

   // Or wrap with scalastyle comments
   // scalastyle:off classforname
   Class.forName("com.example.MyClass")
   // scalastyle:on classforname
   ```

### Verification Checklist

Before committing, verify:

- [ ] Code compiles without errors: `mvn clean verify -Dbuildver=330`
- [ ] Scalastyle passes: `mvn scalastyle:check`
- [ ] Unit tests pass: `mvn test -Dbuildver=330`
- [ ] Integration tests pass: `./integration_tests/runtests.py -k test_acos`
- [ ] No trailing whitespace or missing newlines
- [ ] Line lengths ≤ 100 characters
- [ ] Follows existing code patterns

## Common Pitfalls and Best Practices

### Common Pitfalls

#### 1. Incorrect Type Signatures
```scala
// Bad - doesn't specify what types are supported
expr[MyOp]("My operation", ExprChecks.unaryProject())

// Good - explicitly defines supported types
expr[MyOp]("My operation",
  ExprChecks.unaryProject(
    TypeSig.INT + TypeSig.LONG + TypeSig.DOUBLE,  // GPU supports
    TypeSig.numeric))                              // Spark supports
```

#### 2. Not Handling Nulls
```scala
// Bad - will crash on null input
override def doColumnar(input: GpuColumnVector): ColumnVector = {
  input.getBase.someOperation()  // Fails if input has nulls
}

// Good - cuDF operations handle nulls automatically
override def doColumnar(input: GpuColumnVector): ColumnVector = {
  // cuDF operations preserve null mask automatically
  input.getBase.someOperation()
}
```

#### 3. Memory Leaks
```scala
// Bad - leaks GPU memory
override def doColumnar(input: GpuColumnVector): ColumnVector = {
  val temp = input.getBase.add(Scalar.fromInt(1))
  temp.mul(Scalar.fromInt(2))  // temp is never closed!
}

// Good - use withResource for automatic cleanup
override def doColumnar(input: GpuColumnVector): ColumnVector = {
  withResource(Scalar.fromInt(1)) { one =>
    withResource(input.getBase.add(one)) { temp =>
      withResource(Scalar.fromInt(2)) { two =>
        temp.mul(two)
      }
    }
  }
}
```

#### 4. Incorrect Output Column Order
```scala
// Bad - Spark relies on column order
override def columnarEval(batch: ColumnarBatch): ColumnarBatch = {
  val results = computeResults(batch)
  new ColumnarBatch(results.reverse.toArray)  // Never change order!
}

// Good - preserve column order
override def columnarEval(batch: ColumnarBatch): ColumnarBatch = {
  val results = computeResults(batch)
  new ColumnarBatch(results.toArray)
}
```

### Best Practices

#### 1. Follow Existing Patterns
```scala
// Study similar operators before implementing
// For math operations, look at: mathExpressions.scala
// For string operations, look at: stringExpressions.scala
// For aggregations, look at: aggregate.scala
```

#### 2. Use Appropriate Base Classes

```scala
// For simple cuDF unary operations
case class GpuMyOp(child: Expression) extends CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.MY_OPERATION
}

// For complex custom operations
case class GpuMyOp(child: Expression) extends GpuUnaryExpression {
  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    // Custom cuDF operations
  }
}

// For operations needing AST support
case class GpuMyOp(child: Expression) extends CudfUnaryMathExpression("MYOP") {
  override def unaryOp: UnaryOp = UnaryOp.MY_OPERATION
  override def outputTypeOverride: DType = DType.FLOAT64
}
```

#### 3. Comprehensive Type Checks

```scala
// Specify exactly what types your operator supports
expr[MyOp]("My operation",
  ExprChecks.binaryProject(
    // GPU input types
    (TypeSig.INT + TypeSig.LONG + TypeSig.FLOAT + TypeSig.DOUBLE,
     TypeSig.INT + TypeSig.LONG),
    // Spark input types (what Spark supports)
    (TypeSig.numeric, TypeSig.numeric),
    // GPU output type
    TypeSig.DOUBLE,
    // Spark output type
    TypeSig.DOUBLE))
```

#### 4. Resource Management

Always use `withResource` for GPU objects:
```scala
import com.nvidia.spark.rapids.Arm.{withResource, closeOnExcept}

// Single resource
withResource(Scalar.fromInt(42)) { scalar =>
  column.add(scalar)
}

// Multiple resources
withResource(Scalar.fromInt(1)) { one =>
  withResource(Scalar.fromInt(2)) { two =>
    withResource(column.add(one)) { temp =>
      temp.mul(two)
    }
  }
}

// Exception safety
closeOnExcept(allocateResource()) { resource =>
  doSomething(resource)
}
```

#### 5. Test Incrementally

```bash
# 1. Implement operator
# 2. Build
mvn clean install -Dbuildver=330 -DskipTests

# 3. Test single integration test
cd integration_tests
./runtests.py -k test_my_operator

# 4. Fix issues, rebuild
cd ..
mvn install -pl sql-plugin -am -Dbuildver=330 -DskipTests

# 5. Re-test
cd integration_tests
./runtests.py -k test_my_operator

# 6. Run full test suite when stable
mvn verify -Dbuildver=330
```

#### 6. Documentation and Comments

```scala
// Good - explain non-obvious behavior
case class GpuAcoshCompat(child: Expression) extends GpuUnaryMathExpression("ACOSH") {
  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    // Typically we would just use UnaryOp.ARCCOSH, but there are corner cases
    // where cudf produces a better result (it does not overflow) than Spark does.
    // Our goal is to match Spark's behavior: StrictMath.log(x + math.sqrt(x * x - 1.0))
    val base = input.getBase
    // ... implementation
  }
}
```

## Next Steps

### After Your First Operator

1. **Review Process**:
   - Create a pull request following [CONTRIBUTING.md](../../CONTRIBUTING.md#creating-a-pull-request)
   - Address reviewer feedback
   - Ensure CI passes all checks

2. **Documentation**:
   - Update [supported_ops.md](../supported_ops.md) if adding new functionality
   - Add release notes if user-facing

3. **Configuration** (if needed):
   - Add configuration option in `RapidsConf.scala` if operator should be toggleable
   - Default: enabled

### Resources

- **Developer Overview**: [docs/dev/README.md](README.md)
- **Testing Guide**: [tests/README.md](../../tests/README.md)
- **Shims Documentation**: [docs/dev/shims.md](shims.md)
- **cuDF Documentation**: https://docs.rapids.ai/api/cudf/stable/
- **Apache Spark Internals**: https://books.japila.pl/apache-spark-internals/

### Getting Help

- **Issues**: File questions at https://github.com/NVIDIA/spark-rapids/issues
- **Discussions**: Join discussions at https://github.com/NVIDIA/spark-rapids/discussions
- **Slack**: Join the [RAPIDS Slack](https://rapids.ai/community/) (#spark-rapids channel)

### Progression Path

Follow this path to build expertise systematically:

#### Stage 1: Simple Unary Operators (Week 1-2)
**Goal:** Understand the basics, build process, and testing

Start with 2-3 of these:
- **Math functions**: `sqrt`, `cbrt`, `asin`, `atan`, `tan`, `sinh`
- **String functions**: `upper`, `lower`, `trim`, `ltrim`, `rtrim`
- **Type conversions**: Simple casts between compatible types

**Why start here:**
- Single input/output, easiest to understand
- cuDF has built-in operations
- Minimal state management
- Fast feedback loop

**Resources:**
- [mathExpressions.scala](../../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/mathExpressions.scala)
- [stringExpressions.scala](../../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/stringExpressions.scala)

#### Stage 2: Binary Operators (Week 3-4)
**Goal:** Handle multiple inputs, understand type coercion

Try these operators:
- **Arithmetic**: `divide`, `remainder`, `pmod`
- **String operations**: `concat`, `contains`, `startsWith`, `endsWith`
- **Comparison**: `equalTo`, `greaterThan`, `lessThan`

**New concepts:**
- Type promotion (int + long → long)
- Null handling with two inputs
- Different data types on left vs right

**Resources:**
- [arithmetic.scala](../../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/arithmetic.scala)

#### Stage 3: Aggregate Functions (Week 5-8)
**Goal:** Learn state management, partial aggregation, merging

Recommended order:
1. **Start simple**: `count`, `min`, `max` (stateless aggregation)
2. **Add complexity**: `sum`, `avg` (requires numeric handling)
3. **Advanced**: `collect_list`, `collect_set` (complex data structures)

**New concepts:**
- Initial values and state
- Update vs merge operations
- Handling overflow and ANSI mode
- Group-by aggregation

**Resources:**
- [Complex Operators - Aggregate Example](#example-1-aggregate-function-gpusum) in this guide
- [aggregateFunctions.scala](../../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/aggregate/aggregateFunctions.scala)
- [hash_aggregate_test.py](../../integration_tests/src/main/python/hash_aggregate_test.py)

#### Stage 4: Join Operations (Week 9-12+)
**Goal:** Master multi-table operations, understand broadcast vs shuffle

Recommended progression:
1. **Study existing code** for 1-2 weeks before implementing
2. **Start with**: Understanding `BroadcastHashJoin` implementation
3. **Contribute**: Optimizations or edge case fixes
4. **Advanced**: Implement a new join strategy

**New concepts:**
- Build side vs stream side
- Join types (inner, outer, semi, anti)
- Broadcast variables and data distribution
- Memory management for large tables
- Data skew handling

**Resources:**
- [Complex Operators - Join Example](#example-2-join-operations-broadcasthashjoin) in this guide
- [GpuHashJoin.scala](../../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuHashJoin.scala)
- [GpuBroadcastHashJoinExecBase.scala](../../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuBroadcastHashJoinExecBase.scala)
- [join_test.py](../../integration_tests/src/main/python/join_test.py)

#### Stage 5: Advanced Topics (3+ months)
Once comfortable with Stages 1-4:

- **Window Functions**: Partitioning, ordering, frame boundaries
- **Sort Operations**: Distributed sorting, partition-level sorts
- **File I/O**: Parquet/ORC readers and writers
- **Data Sources**: Custom data source implementations
- **Performance Optimization**: Profile and optimize existing operators

**Resources:**
- [window.scala](../../sql-plugin/src/main/scala/org/apache/spark/sql/rapids/window.scala)
- [data-sources.md](data-sources.md)

### Milestone Checklist

Track your progress:

- [ ] **Beginner** (Stage 1): Completed 2-3 simple unary operators
- [ ] **Intermediate** (Stage 2-3): Completed 2+ binary operators and 1+ aggregate
- [ ] **Advanced** (Stage 4): Understand join implementations, contributed join optimization
- [ ] **Expert** (Stage 5): Contributed window function or data source implementation
- [ ] **Contributor**: Have 5+ merged pull requests
- [ ] **Maintainer**: Help review others' contributions

### Tips for Long-Term Success

1. **Consistency over intensity**: Better to contribute 5 hours/week for 3 months than 40 hours in one week

2. **Document as you learn**: Keep notes on challenges faced and solutions found

3. **Engage with the community**:
   - Comment on issues related to your work
   - Help answer questions from other contributors
   - Share your learnings in discussions

4. **Quality over quantity**:
   - One well-tested, thoroughly documented operator is better than three buggy ones
   - Take time to understand why existing code works the way it does

5. **Celebrate milestones**:
   - First operator merged? Celebrate!
   - Fixed a tricky bug? Share the solution!
   - Helped another contributor? That's valuable too!

---

## Quick Reference Glossary

For when you're deep in the code and need a quick reminder:

### Core Terminology

| Term | What It Means | Example |
|------|---------------|---------|
| **Operator** | Any operation that processes data | `acos()`, `+`, `join`, `sum()` |
| **Expression** | Operations on data values (like functions) | `acos(angle)`, `price + tax` |
| **Physical Plan** | Execution strategy (algorithms) | `HashJoin`, `Sort`, `Scan` |
| **Unary Expression** | One input → one output | `acos(x)`, `sqrt(x)` |
| **Binary Expression** | Two inputs → one output | `a + b`, `a * b` |
| **Columnar** | Processing entire columns at once | Process 1M rows in parallel |
| **cuDF** | GPU DataFrame library (like pandas) | Provides GPU operations |
| **TypeSig** | Defines supported data types | `INT`, `LONG`, `DOUBLE`, `STRING` |
| **GpuOverrides** | Central registry of CPU→GPU mappings | Where you register operators |
| **RapidsMeta** | Validation wrapper for operators | Checks if GPU can be used |
| **withResource** | Automatic GPU memory cleanup | Prevents memory leaks |

### Key Classes

| Class | Purpose | When to Use |
|-------|---------|-------------|
| **Simple Expressions** | | |
| `GpuExpression` | Base for all GPU expressions | Extend for custom operators |
| `GpuUnaryExpression` | Base for unary operations | Single input operators |
| `GpuBinaryExpression` | Base for binary operations | Two input operators |
| `CudfUnaryExpression` | Unary with cuDF UnaryOp | When cuDF has the operation |
| `CudfUnaryMathExpression` | Math unary with AST support | Math functions |
| `UnaryExprMeta[T]` | Metadata for unary expressions | Validation and tagging |
| `BinaryExprMeta[T]` | Metadata for binary expressions | Validation and tagging |
| **Complex Operators** | | |
| `AggExprMeta[T]` | Metadata for aggregate expressions | Sum, count, avg, etc. |
| `GpuAggregateFunction` | Base for GPU aggregates | Custom aggregate functions |
| `CudfAggregate` | cuDF aggregate wrapper | Delegates to cuDF aggregations |
| `SparkPlanMeta[T]` | Metadata for physical plans | Joins, sorts, scans, etc. |
| `GpuExec` | Base for GPU physical operators | Physical execution operators |
| `GpuBuildSide` | Join build side indicator | Which side to build hash table |
| **Data Structures** | | |
| `GpuColumnVector` | Wraps cuDF Column | Access GPU data |
| `ColumnVector` | cuDF's column data structure | Result of GPU operations |
| `ColumnarBatch` | Batch of columnar data | Multiple columns together |

### File Locations

| What You Need | Where to Look |
|---------------|---------------|
| **Expressions** | |
| Math operators | `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/mathExpressions.scala` |
| Arithmetic operators | `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/arithmetic.scala` |
| String operators | `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/stringExpressions.scala` |
| **Complex Operators** | |
| Aggregate functions | `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/aggregate/aggregateFunctions.scala` |
| Aggregate base classes | `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/aggregate/aggregateBase.scala` |
| Join operations | `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuHashJoin.scala` |
| Broadcast hash join | `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/execution/GpuBroadcastHashJoinExecBase.scala` |
| **Core Files** | |
| Registration | `sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuOverrides.scala` |
| Type signatures | `sql-plugin/src/main/scala/com/nvidia/spark/rapids/TypeSig.scala` |
| **Testing** | |
| Integration tests | `integration_tests/src/main/python/*_test.py` |
| Join tests | `integration_tests/src/main/python/join_test.py` |
| Aggregate tests | `integration_tests/src/main/python/hash_aggregate_test.py` |

### Common Patterns

```scala
// Registering an operator in GpuOverrides.scala
expr[SparkOperatorName](
  "Description",
  ExprChecks.appropriateCheck,
  (a, conf, p, r) => new UnaryExprMeta[SparkOperatorName](a, conf, p, r) {
    override def convertToGpu(child: Expression): GpuExpression =
      GpuOperatorName(child)
  })

// Simple cuDF unary operator
case class GpuMyOp(child: Expression) extends CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.OPERATION_NAME
}

// Custom implementation
case class GpuMyOp(child: Expression) extends GpuUnaryExpression {
  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(input.getBase) { col =>
      // Your cuDF operations here
    }
  }
}

// Memory management
withResource(Scalar.fromInt(value)) { scalar =>
  withResource(column.operation(scalar)) { result =>
    result.copyToHost()
  }
}
```

### Build Commands Cheat Sheet

```bash
# Full build with tests (Spark 3.3.0)
mvn clean verify -Dbuildver=330

# Quick build without tests
mvn clean install -Dbuildver=330 -DskipTests

# Check code style
mvn scalastyle:check

# Run specific test
mvn test -Dbuildver=330 -Dtest=RapidsMathFunctionsSuite

# Run integration test
cd integration_tests && ./runtests.py -k test_acos

# Build specific module only
mvn install -pl sql-plugin -am -Dbuildver=330 -DskipTests
```

### Debug Checklist

When things don't work:

- [ ] Did you register in `GpuOverrides.scala`?
- [ ] Does the type signature match your data?
- [ ] Are you using `withResource` for GPU memory?
- [ ] Does the cuDF operation exist? (Check cuDF docs)
- [ ] Did you add the import: `import ai.rapids.cudf._`?
- [ ] Is scalastyle passing? (`mvn scalastyle:check`)
- [ ] Are you testing with the right Spark version?
- [ ] Did you rebuild after changes? (`mvn install -DskipTests`)

### Need Help?

**First, check:**
1. Existing similar operators in the same file
2. Error messages (they're usually informative!)
3. This guide's examples

**Then ask:**
- File an issue with your question
- Include: what you're trying to do, what you've tried, error messages
- Be patient - the community is helpful but volunteers

---

**Happy contributing! 🚀** Remember: start small, test thoroughly, and follow existing patterns.
