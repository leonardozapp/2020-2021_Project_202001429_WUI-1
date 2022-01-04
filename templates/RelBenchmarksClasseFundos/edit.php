<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\RelBenchmarksClasseFundo $relBenchmarksClasseFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $relBenchmarksClasseFundo->tipo_benchmark_id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $relBenchmarksClasseFundo->tipo_benchmark_id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Rel Benchmarks Classe Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="relBenchmarksClasseFundos form content">
            <?= $this->Form->create($relBenchmarksClasseFundo) ?>
            <fieldset>
                <legend><?= __('Edit Rel Benchmarks Classe Fundo') ?></legend>
                <?php
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
