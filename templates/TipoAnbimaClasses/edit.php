<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoAnbimaClass $tipoAnbimaClass
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $tipoAnbimaClass->id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $tipoAnbimaClass->id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Tipo Anbima Classes'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoAnbimaClasses form content">
            <?= $this->Form->create($tipoAnbimaClass) ?>
            <fieldset>
                <legend><?= __('Edit Tipo Anbima Class') ?></legend>
                <?php
                    echo $this->Form->control('classe_anbima');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
