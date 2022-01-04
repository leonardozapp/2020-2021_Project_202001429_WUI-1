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
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $tipoBenchmark->id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $tipoBenchmark->id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Tipo Benchmarks'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoBenchmarks form content">
            <?= $this->Form->create($tipoBenchmark) ?>
            <fieldset>
                <legend><?= __('Edit Tipo Benchmark') ?></legend>
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
