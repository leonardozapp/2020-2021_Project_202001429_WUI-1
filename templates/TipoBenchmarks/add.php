<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoBenchmark $tipoBenchmark
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('List Tipo Benchmarks'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoBenchmarks form content">
            <?= $this->Form->create($tipoBenchmark) ?>
            <fieldset>
                <legend><?= __('Add Tipo Benchmark') ?></legend>
                <?php
                    echo $this->Form->control('nome');
                    echo $this->Form->control('sigla');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
