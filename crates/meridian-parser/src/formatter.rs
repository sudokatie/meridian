//! Code formatter for Meridian.
//!
//! Pretty-prints AST back to formatted source code.

use crate::ast::*;

/// Format a program to a string.
pub fn format_program(program: &Program) -> String {
    let mut output = String::new();
    
    for (i, item) in program.items.iter().enumerate() {
        if i > 0 {
            output.push_str("\n\n");
        }
        output.push_str(&format_item(item));
    }
    
    if !output.is_empty() {
        output.push('\n');
    }
    
    output
}

fn format_item(item: &Item) -> String {
    match item {
        Item::Schema(schema) => format_schema(schema),
        Item::Source(source) => format_source(source),
        Item::Stream(stream) => format_stream(stream),
        Item::Sink(sink) => format_sink(sink),
        Item::Pipeline(pipeline) => format_pipeline(pipeline),
        Item::Function(func) => format_function(func),
        Item::Test(test) => format_test(test),
    }
}

fn format_stream(stream: &StreamSource) -> String {
    let mut out = format!(
        "stream {} from {}(\"{}\") {{",
        stream.name.name, stream.source_type, stream.path
    );
    if !stream.config.is_empty() {
        out.push('\n');
        for (key, value) in &stream.config {
            out.push_str(&format!("    {}: {}\n", key.name, format_expr(value)));
        }
    }
    out.push('}');
    out
}

fn format_duration(d: &Duration) -> String {
    let unit = match d.unit {
        DurationUnit::Seconds => "seconds",
        DurationUnit::Minutes => "minutes",
        DurationUnit::Hours => "hours",
        DurationUnit::Days => "days",
    };
    format!("{}.{}", d.value, unit)
}

fn format_schema(schema: &Schema) -> String {
    let mut out = format!("schema {} {{\n", schema.name.name);
    for field in &schema.fields {
        out.push_str(&format!("    {}: {}\n", field.name.name, format_type(&field.ty)));
    }
    out.push('}');
    out
}

fn format_type(ty: &TypeExpr) -> String {
    match ty {
        TypeExpr::Named(ident) => ident.name.clone(),
        TypeExpr::Decimal { precision, scale, .. } => format!("decimal({}, {})", precision, scale),
        TypeExpr::List(inner, _) => format!("list<{}>", format_type(inner)),
        TypeExpr::Map(key, value, _) => format!("map<{}, {}>", format_type(key), format_type(value)),
        TypeExpr::Struct(fields, _) => {
            let field_strs: Vec<_> = fields
                .iter()
                .map(|(name, ty)| format!("{}: {}", name.name, format_type(ty)))
                .collect();
            format!("struct {{ {} }}", field_strs.join(", "))
        }
        TypeExpr::Enum(values, _) => {
            let vals: Vec<_> = values.iter().map(|v| format!("\"{}\"", v)).collect();
            format!("enum({})", vals.join(", "))
        }
        TypeExpr::Nullable(inner, _) => format!("nullable<{}>", format_type(inner)),
    }
}

fn format_source(source: &Source) -> String {
    let mut out = format!(
        "source {} from {}(\"{}\")",
        source.name.name, source.source_type, source.path
    );
    
    if !source.config.is_empty() {
        out.push_str(" {\n");
        for (key, value) in &source.config {
            out.push_str(&format!("    {}: {}\n", key.name, format_expr(value)));
        }
        out.push('}');
    }
    
    out
}

fn format_sink(sink: &Sink) -> String {
    let mut out = format!(
        "sink {} to {}(\"{}\")",
        sink.pipeline.name, sink.sink_type, sink.path
    );
    
    if !sink.config.is_empty() {
        out.push_str(" {\n");
        for (key, value) in &sink.config {
            out.push_str(&format!("    {}: {}\n", key.name, format_expr(value)));
        }
        out.push('}');
    }
    
    out
}

fn format_pipeline(pipeline: &Pipeline) -> String {
    let mut out = format!("pipeline {} {{\n", pipeline.name.name);
    for stmt in &pipeline.statements {
        out.push_str(&format!("    {}\n", format_statement(stmt)));
    }
    out.push('}');
    out
}

fn format_statement(stmt: &Statement) -> String {
    match stmt {
        Statement::From(f) => format!("from {}", f.source.name),
        Statement::Where(w) => format!("where {}", format_expr(&w.condition)),
        Statement::Select(s) => {
            let fields: Vec<_> = s.fields.iter().map(format_select_field).collect();
            if fields.len() <= 3 {
                format!("select {{ {} }}", fields.join(", "))
            } else {
                let mut out = String::from("select {\n");
                for field in fields {
                    out.push_str(&format!("        {},\n", field));
                }
                out.push_str("    }");
                out
            }
        }
        Statement::GroupBy(g) => {
            let keys: Vec<_> = g.keys.iter().map(format_expr).collect();
            format!("group by {}", keys.join(", "))
        }
        Statement::OrderBy(o) => {
            let keys: Vec<_> = o.keys.iter().map(|(e, order)| {
                let dir = match order {
                    SortOrder::Asc => "",
                    SortOrder::Desc => " desc",
                };
                format!("{}{}", format_expr(e), dir)
            }).collect();
            format!("order by {}", keys.join(", "))
        }
        Statement::Limit(l) => format!("limit {}", l.count),
        Statement::Join(j) => {
            let kind = match j.kind {
                JoinKind::Inner => "",
                JoinKind::Left => "left ",
                JoinKind::Right => "right ",
                JoinKind::Full => "full ",
            };
            let within = j.within.as_ref()
                .map(|d| format!(" within {}", format_duration(d)))
                .unwrap_or_default();
            format!("{}join {}{} on {}", kind, j.source.name, within, format_expr(&j.condition))
        }
        Statement::Union(u) => format!("union {}", u.pipeline.name),
        Statement::Window(w) => {
            let window_type = match &w.window_type {
                WindowType::Tumbling(d) => format!("tumbling({})", format_duration(d)),
                WindowType::Sliding { size, slide } => {
                    format!("sliding({}, {})", format_duration(size), format_duration(slide))
                }
                WindowType::Session(d) => format!("session({})", format_duration(d)),
            };
            format!("window {} on {}", window_type, w.time_column.name)
        }
        Statement::Emit(e) => {
            let mode = match e.config.mode {
                EmitMode::Final => "final",
                EmitMode::Updates => "updates",
                EmitMode::Append => "append",
            };
            let lateness = e.config.allowed_lateness.as_ref()
                .map(|d| format!(", allowed_lateness: {}", format_duration(d)))
                .unwrap_or_default();
            format!("emit {{ mode: {}{} }}", mode, lateness)
        }
    }
}

fn format_select_field(field: &SelectField) -> String {
    match field {
        SelectField::Named { name, expr, .. } => {
            format!("{}: {}", name.name, format_expr(expr))
        }
        SelectField::Expr { expr, .. } => format_expr(expr),
        SelectField::Spread { source, .. } => format!("...{}", source.name),
    }
}

fn format_function(func: &Function) -> String {
    let params: Vec<_> = func.params.iter()
        .map(|p| format!("{}: {}", p.name.name, format_type(&p.ty)))
        .collect();
    
    format!(
        "fn {}({}) -> {} {{\n    {}\n}}",
        func.name.name,
        params.join(", "),
        format_type(&func.return_type),
        format_expr(&func.body)
    )
}

fn format_test(test: &Test) -> String {
    let mut out = format!("test \"{}\" {{\n", test.name);
    for stmt in &test.body {
        match stmt {
            TestStatement::Assert(expr, _) => {
                out.push_str(&format!("    assert {}\n", format_expr(expr)));
            }
            TestStatement::Given { name, value, .. } => {
                out.push_str(&format!("    given {} = {}\n", name.name, format_expr(value)));
            }
            TestStatement::Expect { pipeline, value, .. } => {
                out.push_str(&format!("    expect {} == {}\n", pipeline.name, format_expr(value)));
            }
        }
    }
    out.push('}');
    out
}

fn format_expr(expr: &Expr) -> String {
    match expr {
        Expr::Int(n, _) => n.to_string(),
        Expr::Float(n, _) => format!("{}", n),
        Expr::String(s, _) => format!("\"{}\"", s),
        Expr::Bool(b, _) => b.to_string(),
        Expr::List(elements, _) => {
            let els: Vec<_> = elements.iter().map(format_expr).collect();
            format!("[{}]", els.join(", "))
        }
        Expr::Duration(d) => format_duration(d),
        Expr::Ident(id) => id.name.clone(),
        Expr::Field(base, field, _) => format!("{}.{}", format_expr(base), field.name),
        Expr::Binary(left, op, right, _) => {
            let op_str = match op {
                BinOp::Add => "+",
                BinOp::Sub => "-",
                BinOp::Mul => "*",
                BinOp::Div => "/",
                BinOp::Mod => "%",
                BinOp::Eq => "==",
                BinOp::Ne => "!=",
                BinOp::Lt => "<",
                BinOp::Le => "<=",
                BinOp::Gt => ">",
                BinOp::Ge => ">=",
                BinOp::And => "and",
                BinOp::Or => "or",
                BinOp::Concat => "++",
            };
            format!("{} {} {}", format_expr(left), op_str, format_expr(right))
        }
        Expr::Unary(op, inner, _) => {
            match op {
                UnaryOp::Neg => format!("-{}", format_expr(inner)),
                UnaryOp::Not => format!("not {}", format_expr(inner)),
                UnaryOp::IsNull => format!("{} is null", format_expr(inner)),
                UnaryOp::IsNotNull => format!("{} is not null", format_expr(inner)),
            }
        }
        Expr::Call(name, args, _) => {
            let arg_strs: Vec<_> = args.iter().map(format_expr).collect();
            format!("{}({})", name.name, arg_strs.join(", "))
        }
        Expr::Match(arms, default, _) => {
            let mut out = String::from("match {\n");
            for (pattern, result) in arms {
                out.push_str(&format!("        {} -> {},\n", format_expr(pattern), format_expr(result)));
            }
            if let Some(def) = default {
                out.push_str(&format!("        _ -> {}\n", format_expr(def)));
            }
            out.push_str("    }");
            out
        }
        Expr::NullCoalesce(left, right, _) => {
            format!("{} ?? {}", format_expr(left), format_expr(right))
        }
        Expr::NonNullAssert(inner, _) => {
            format!("{}!", format_expr(inner))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;

    #[test]
    fn test_format_simple_pipeline() {
        let source = r#"
            pipeline example {
                from orders
                where amount > 100
                select { id, amount }
            }
        "#;
        let program = parse(source).unwrap();
        let formatted = format_program(&program);
        
        assert!(formatted.contains("pipeline example"));
        assert!(formatted.contains("from orders"));
        assert!(formatted.contains("where amount > 100"));
    }

    #[test]
    fn test_format_schema() {
        let source = r#"
            schema Order {
                id: string
                amount: decimal(10, 2)
            }
        "#;
        let program = parse(source).unwrap();
        let formatted = format_program(&program);
        
        assert!(formatted.contains("schema Order"));
        assert!(formatted.contains("id: string"));
        assert!(formatted.contains("amount: decimal(10, 2)"));
    }

    #[test]
    fn test_format_function() {
        let source = r#"
            fn double(x: int) -> int {
                x * 2
            }
        "#;
        let program = parse(source).unwrap();
        let formatted = format_program(&program);
        
        assert!(formatted.contains("fn double(x: int) -> int"));
        assert!(formatted.contains("x * 2"));
    }
}
