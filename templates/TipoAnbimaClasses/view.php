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
            <?= $this->Html->link(__('Edit Tipo Anbima Class'), ['action' => 'edit', $tipoAnbimaClass->id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Tipo Anbima Class'), ['action' => 'delete', $tipoAnbimaClass->id], ['confirm' => __('Are you sure you want to delete # {0}?', $tipoAnbimaClass->id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Tipo Anbima Classes'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Tipo Anbima Class'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoAnbimaClasses view content">
            <h3><?= h($tipoAnbimaClass->id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Classe Anbima') ?></th>
                    <td><?= h($tipoAnbimaClass->classe_anbima) ?></td>
                </tr>
                <tr>
                    <th><?= __('Id') ?></th>
                    <td><?= $this->Number->format($tipoAnbimaClass->id) ?></td>
                </tr>
            </table>
        </div>
    </div>
</div>
