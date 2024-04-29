#!/usr/bin/env nextflow

process sayHello {
  input: 
    val x
  output:
    path "${x}.txt"
  publishDir "results/", mode: 'copy'
  script:
    """
    echo '$x world!' > ${x}.txt
    sleep 60
    """
}

workflow {
  Channel.of('Bonjour', 'Ciao', 'Hello', 'Hola') | sayHello | view{it.text}
}
